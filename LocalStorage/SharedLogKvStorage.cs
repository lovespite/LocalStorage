namespace LocalStorage;

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;

/// <summary>
/// 支持多进程并发读写的日志型键值存储。
/// <para>采用 Actor 模型（单线程任务队列）串行化 IO 操作。</para>
/// <para>引入独立的 Lock File (.lock) 机制，确保在数据文件 Compact/Rotate 期间锁依然有效。</para>
/// </summary>
public class SharedLogKvStorage : KeyValueStorage, IDisposable
{
    private readonly string _dataFilePath;
    private readonly string _lockFilePath;

    // 数据文件流 (可能会在 Compact 时关闭并重新打开)
    private FileStream? _dfs;
    private FileStream? DataFileStream
    {
        get => _dfs;
        set => _dfs = value;
    }

    // 锁文件流 (生命周期内常驻，用于跨进程互斥)
    private FileStream? _lfs;

    private readonly ConcurrentDictionary<string, string> _memoryIndex;

    // --- 任务队列相关 ---
    private readonly Channel<Action> _taskQueue;
    private readonly Task _workerTask;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    // 记录当前进程已加载的文件位置（用于增量同步）
    private long _lastSyncedPosition = 0;

    // 锁文件中的锁定位置（由于是专用锁文件，直接锁第0位即可）
    private const long LOCK_POSITION = 0;
    private const long LOCK_LENGTH = 1;

    private const byte OP_SET = 1;
    private const byte OP_DEL = 2;

    public long FileSize => DataFileStream?.Length ?? 0;

    public override string Name => Path.GetFileNameWithoutExtension(_dataFilePath);

    private SharedLogKvStorage(string filePath, ConcurrentDictionary<string, string> memoryIndex)
    {
        _dataFilePath = filePath;
        _lockFilePath = filePath + ".lock";
        _memoryIndex = memoryIndex;

        // 初始化任务队列
        _taskQueue = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _cts = new CancellationTokenSource();

        // 启动专用工作线程
        _workerTask = Task.Factory.StartNew(
            ProcessQueueLoop,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach
        );
    }

    public static SharedLogKvStorage Open(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath)) throw new ArgumentNullException(nameof(filePath));
        var fName = Path.GetFileNameWithoutExtension(filePath);

        var dir = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

        var memoryIndex = new ConcurrentDictionary<string, string>();
        var storage = new SharedLogKvStorage(filePath, memoryIndex);

        // 通过队列初始化文件句柄和同步数据，确保所有文件操作都在 Actor 线程中
        storage.EnqueueOperationAsync(() =>
        {
            storage.InitializeStreams();
            storage.SyncDataInternal();
        }).GetAwaiter().GetResult();

        return storage;
    }

    /// <summary>
    /// 初始化或重新打开文件流
    /// </summary>
    private void InitializeStreams()
    {
        try
        {
            _lfs ??= new FileStream(
                    _lockFilePath
                    , FileMode.OpenOrCreate
                    , FileAccess.ReadWrite
                    , FileShare.ReadWrite
                    , bufferSize: 4096
                    // ,FileOptions.DeleteOnClose
                    );

            _dfs ??= new FileStream(
                    _dataFilePath
                    , FileMode.OpenOrCreate
                    , FileAccess.ReadWrite
                    , FileShare.ReadWrite
                    );
        }
        catch
        {
            // 清理已打开的资源
            _lfs?.Dispose();
            _lfs = null;
            _dfs?.Dispose();
            _dfs = null;
            throw;
        }
    }

    /// <summary>
    /// 专用的工作线程循环（Actor Loop）
    /// </summary>
    private async Task ProcessQueueLoop()
    {
        var reader = _taskQueue.Reader;
        try
        {
            while (await reader.WaitToReadAsync(_cts.Token))
            {
                while (reader.TryRead(out var action))
                {
                    try
                    {
                        action();
                    }
                    catch (Exception ex)
                    {
                        LogError($"Critical error in file loop: {ex}");
                        // 不抛出异常，保持循环存活，除非是致命的系统错误
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // 正常退出
        }
        catch (Exception ex)
        {
            LogError($"Actor loop crash: {ex}");
        }
    }

    /// <summary>
    /// 将操作入队并等待结果
    /// </summary>
    private Task<T> EnqueueOperationAsync<T>(Func<T> operation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        bool success = _taskQueue.Writer.TryWrite(() =>
        {
            try
            {
                var result = operation();
                tcs.SetResult(result);
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }
        });

        if (!success)
        {
            tcs.SetException(new InvalidOperationException("Storage queue is full or closed."));
        }

        return tcs.Task;
    }

    private Task EnqueueOperationAsync(Action operation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        bool success = _taskQueue.Writer.TryWrite(() =>
        {
            try
            {
                operation();
                tcs.SetResult();
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }
        });

        if (!success)
        {
            tcs.SetException(new InvalidOperationException("Storage queue is full or closed."));
        }

        return tcs.Task;
    }

    #region Internal File Operations (Must run in Actor Thread)

    private void EnterWriteLock()
    {
        if (_lfs is null) throw new InvalidOperationException("Lock stream is not initialized.");

        // 自旋等待锁
        while (true)
        {
            try
            {
                // 锁住 .lock 文件的第 0 字节
                _lfs.Lock(LOCK_POSITION, LOCK_LENGTH);
                return;
            }
            catch (IOException)
            {
                // 锁被占用，稍后重试
                Thread.Sleep(5);
            }
        }
    }

    private void ExitWriteLock()
    {
        try
        {
            _lfs?.Unlock(LOCK_POSITION, LOCK_LENGTH);
        }
        catch (Exception ex)
        {
            LogError($"Failed to release lock: {ex.Message}");
        }
    }

    private void SyncDataInternal()
    {
        if (DataFileStream is null) return;

        long currentFileLength = DataFileStream.Length;
        if (currentFileLength == _lastSyncedPosition)
        {
            return;
        }

        DataFileStream.Seek(_lastSyncedPosition, SeekOrigin.Begin);

        using var reader = new BinaryReader(DataFileStream, Encoding.UTF8, leaveOpen: true);

        try
        {
            while (DataFileStream.Position < currentFileLength)
            {
                long recordStart = DataFileStream.Position;
                // 防止读取不完整的尾部垃圾数据（如果崩溃导致写入一半）
                if (currentFileLength - recordStart < 5) break;

                try
                {
                    byte op = reader.ReadByte();
                    string key = reader.ReadString();

                    if (op == OP_SET)
                    {
                        string value = reader.ReadString();
                        _memoryIndex[key] = value;
                    }
                    else if (op == OP_DEL)
                    {
                        _memoryIndex.TryRemove(key, out _);
                    }

                    // 成功读取一条完整记录后，才更新同步位置
                    _lastSyncedPosition = DataFileStream.Position;
                }
                catch (EndOfStreamException)
                {
                    // 读取到意外的文件末尾，停止同步
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            LogError($"SyncData error: {ex.Message}");
            // 发生异常时，回滚位置，下次再试
            DataFileStream.Seek(_lastSyncedPosition, SeekOrigin.Begin);
        }
    }

    private void WriteSetInternal(Stream stream, string key, string json)
    {
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        writer.Write(OP_SET);
        writer.Write(key);
        writer.Write(json);
    }

    private void LogError(string message)
    {
        // 简单日志，实际项目中应接入 ILogger
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [SharedLogKvStorage] {message}");
    }

    #endregion

    #region Public APIs (Async)

    public override Task SetAsync(string key, string json)
    {
        ArgumentException.ThrowIfNullOrEmpty(key, nameof(key));
        // ArgumentException.ThrowIfNullOrEmpty(json, nameof(json));

        return EnqueueOperationAsync(() =>
        {
            try
            {
                EnterWriteLock();
                SyncDataInternal();

                if (DataFileStream is null) throw new InvalidOperationException("Stream closed.");

                DataFileStream.Seek(0, SeekOrigin.End);
                WriteSetInternal(DataFileStream, key, json);
                DataFileStream.Flush();

                _memoryIndex[key] = json;
                _lastSyncedPosition = DataFileStream.Position;
            }
            finally
            {
                ExitWriteLock();
            }
        });
    }

    public override Task SetBatchAsync(ICollection<KeyValuePair<string, string>> data)
    {
        ArgumentNullException.ThrowIfNull(data, nameof(data));
        return EnqueueOperationAsync(() =>
        {
            try
            {
                EnterWriteLock();
                SyncDataInternal();
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed.");
                DataFileStream.Seek(0, SeekOrigin.End);
                foreach (var kvp in data)
                {
                    WriteSetInternal(DataFileStream, kvp.Key, kvp.Value);
                    _memoryIndex[kvp.Key] = kvp.Value;
                }
                DataFileStream.Flush();
                _lastSyncedPosition = DataFileStream.Position;
            }
            finally
            {
                ExitWriteLock();
            }
        });
    }

    public override Task<string?> GetAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return EnqueueOperationAsync(() =>
        {
            try
            {
                SyncDataInternal();
            }
            catch (Exception ex)
            {
                LogError($"GetAsync auto-sync failed: {ex.Message}");
            }

            return _memoryIndex.TryGetValue(key, out var val) ? val : null;
        });
    }

    public override Task<ICollection<KeyValuePair<string, string>>> GetBatchAsync(ICollection<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys, nameof(keys));
        return EnqueueOperationAsync<ICollection<KeyValuePair<string, string>>>(() =>
        {
            try
            {
                SyncDataInternal();
            }
            catch (Exception ex)
            {
                LogError($"GetBatchAsync auto-sync failed: {ex.Message}");
            }
            var result = new List<KeyValuePair<string, string>>();
            foreach (var key in keys)
            {
                if (_memoryIndex.TryGetValue(key, out var val))
                {
                    result.Add(new KeyValuePair<string, string>(key, val));
                }
            }
            return result;
        });
    }

    public override Task RemoveAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return EnqueueOperationAsync(() =>
        {
            if (!_memoryIndex.ContainsKey(key)) return;

            try
            {
                EnterWriteLock();
                SyncDataInternal();

                if (DataFileStream is null) throw new InvalidOperationException("Stream closed.");

                DataFileStream.Seek(0, SeekOrigin.End);
                using var writer = new BinaryWriter(DataFileStream, Encoding.UTF8, leaveOpen: true);
                writer.Write(OP_DEL);
                writer.Write(key);
                DataFileStream.Flush();

                _memoryIndex.TryRemove(key, out _);
                _lastSyncedPosition = DataFileStream.Position;
            }
            finally
            {
                ExitWriteLock();
            }
        });
    }

    public override Task ClearAsync()
    {
        return EnqueueOperationAsync(() =>
        {
            try
            {
                EnterWriteLock();
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed.");

                DataFileStream.SetLength(0);
                DataFileStream.Flush();

                _memoryIndex.Clear();
                _lastSyncedPosition = 0;
            }
            finally
            {
                ExitWriteLock();
            }
        });
    }

    public override Task<bool> HasKeyAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return EnqueueOperationAsync(() =>
        {
            try
            {
                SyncDataInternal();
            }
            catch { }
            return _memoryIndex.ContainsKey(key);
        });
    }

    public override Task<ICollection<string>> GetKeysAsync()
    {
        return EnqueueOperationAsync<ICollection<string>>(() =>
        {
            try
            {
                SyncDataInternal();
            }
            catch { }
            return [.. _memoryIndex.Keys];
        });
    }

    #endregion 

    /// <summary>
    /// 压缩数据文件（Atomic Compact）
    /// <para>重写：现在使用独立的锁文件，允许在持有锁的同时安全地关闭并替换数据文件。</para>
    /// </summary>
    public void Compact()
    {
        EnqueueOperationAsync(() =>
        {
            bool lockTaken = false;
            try
            {
                EnterWriteLock();
                lockTaken = true;

                // 1. 同步最新数据
                SyncDataInternal();

                // 2. 准备临时文件
                var tempFilePath = _dataFilePath + ".tmp";

                // 确保临时文件是新的
                if (File.Exists(tempFilePath)) File.Delete(tempFilePath);

                using (var tempFs = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                using (var writer = new BinaryWriter(tempFs, Encoding.UTF8, leaveOpen: true))
                {
                    foreach (var kvp in _memoryIndex)
                    {
                        writer.Write(OP_SET);
                        writer.Write(kvp.Key);
                        writer.Write(kvp.Value);
                    }
                    tempFs.Flush();
                }

                // 3. 关键步骤：原子替换
                // 关闭原数据流 (锁依然由 _lockFileStream 保持，其他进程无法进入)
                DataFileStream?.Dispose();
                DataFileStream = null;

                // 原子移动/替换 (Windows下 Replace 是原子的，Move+Overwrite 也是不错的选择)
                // 使用 Move 覆盖原文件
                File.Move(tempFilePath, _dataFilePath, overwrite: true);

                // 4. 重新打开数据流
                DataFileStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                _lastSyncedPosition = DataFileStream.Length; // 重新定位到末尾
            }
            catch (Exception ex)
            {
                LogError($"Compact failed: {ex}");
                // 尝试恢复数据流
                if (DataFileStream is null)
                {
                    try
                    {
                        DataFileStream = new FileStream(_dataFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                        DataFileStream.Seek(0, SeekOrigin.End);
                        _lastSyncedPosition = DataFileStream.Length;
                    }
                    catch { /* 灾难性错误，无法恢复 */ }
                }
            }
            finally
            {
                if (lockTaken) ExitWriteLock();
            }
        }).GetAwaiter().GetResult();
    }

    #region Dispose Pattern

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                // 停止接收新任务
                _taskQueue.Writer.TryComplete();
                _cts.Cancel();

                // 等待工作线程安全退出
                try
                {
                    _workerTask.Wait(TimeSpan.FromSeconds(2));
                }
                catch { }

                _cts.Dispose();

                // 释放文件资源
                _dfs?.Dispose();
                _lfs?.Dispose(); // 释放锁文件句柄，锁自动释放
                try
                {
                    File.Delete(_lockFilePath); // 删除锁文件
                }
                catch
                {
                    // 忽略删除失败，有可能其他实例仍在使用
                }
            }

            _disposed = true;
        }
    }

    public override void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    ~SharedLogKvStorage()
    {
        Dispose(disposing: false);
    }

    #endregion
}