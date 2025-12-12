namespace LocalStorage;

using LocalStorage.Tools;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Channels;

/// <summary>
/// 支持多进程并发读写的日志型键值存储。
/// <para>采用 Actor 模型（单线程任务队列）串行化 IO 操作。</para>
/// <para>引入 CRC32 校验保证数据完整性。</para>
/// <para>内存仅存储文件指针（索引），大幅降低内存占用。</para>
/// </summary>
public class SharedLogKvStorage : KeyValueStorage, IDisposable
{
    public const int MAX_RECORD_PAYLOAD_SIZE = 100 * 1024 * 1024; // 100 MB 

    private readonly FileStream _dfs;
    private readonly FileStream _lfs;
    private readonly FileLock _fLock;
    private FileStream DataFileStream => _dfs;

    // 修改：内存索引只存储文件指针，不再存储完整的 Value 字符串
    private readonly ConcurrentDictionary<string, ValuePointer> _memoryIndex;

    // --- 任务队列相关 ---
    private readonly Channel<Action> _taskQueue;
    private readonly Task _workerTask;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    private long _lastSyncedPosition = 0;

    private const byte OP_NOP = 0;
    private const byte OP_SET = 1;
    private const byte OP_DEL = 2;

    // 数据头长度：Length(4) + CRC(4) = 8
    private const int HEADER_SIZE = 8;

    public long FileSize => DataFileStream?.Length ?? 0;
    public override bool IsDisposed => _disposed;

    public override string Name => Path.GetFileNameWithoutExtension(_dfs.Name);

    /// <summary>
    /// 指向文件内部 Value 位置的指针结构
    /// </summary>
    private readonly struct ValuePointer(long offset)
    {
        public readonly long Offset = offset; // Value 在文件中的起始偏移量（包含长度前缀）
        public static readonly ValuePointer Null = new(-1);
    }

    private SharedLogKvStorage(FileStream dfs, FileStream lfs)
    {
        //_dataFilePath = dfs.Name; 
        //_lockFilePath = lfs.Name;
        _dfs = dfs; _lfs = lfs;
        _lastSyncedPosition = 0;
        _fLock = new FileLock(_lfs.SafeFileHandle, Name);

        // 初始化内存索引
        _memoryIndex = new ConcurrentDictionary<string, ValuePointer>();

        // 初始化任务队列
        _taskQueue = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        // 启动处理任务队列的后台任务
        _cts = new CancellationTokenSource();
        _workerTask = Task.Factory.StartNew(
            ProcessQueueLoop,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach
        );
    }

    public static SharedLogKvStorage Open(string dataFilePath)
    {
        var dir = Path.GetDirectoryName(dataFilePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

        var lockFilePath = Path.ChangeExtension(dataFilePath, ".lock");
        FileStream? lfs = null, dfs = null;

        // 初始化文件流
        try
        {
            lfs = new FileStream(lockFilePath
                               , FileMode.OpenOrCreate
                               , FileAccess.ReadWrite
                               , FileShare.ReadWrite
                               , bufferSize: 1 // No buffering needed for lock file
                               );
            dfs = new FileStream(dataFilePath
                               , FileMode.OpenOrCreate
                               , FileAccess.ReadWrite
                               , FileShare.ReadWrite
                               , bufferSize: 4096
                               );
        }
        catch
        {
            lfs?.Dispose();
            dfs?.Dispose();
            throw;
        }

        var storage = new SharedLogKvStorage(dfs, lfs);

        storage.SyncDataInternal();

        return storage;
    }

    private async Task ProcessQueueLoop()
    {
        var reader = _taskQueue.Reader;
        try
        {
            while (await reader.WaitToReadAsync(_cts.Token))
            {
                while (reader.TryRead(out var action))
                {
                    try { action(); }
                    catch (Exception ex) { LogError($"Critical error in file loop: {ex}"); }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { LogError($"Actor loop crash: {ex}"); }
    }

    private Task<T> EnqueueOperationAsync<T>(Func<T> operation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_taskQueue.Writer.TryWrite(() =>
        {
            try { tcs.SetResult(operation()); }
            catch (Exception ex) { tcs.SetException(ex); }
        }))
        {
            tcs.SetException(new InvalidOperationException("Storage queue is closed."));
        }
        return tcs.Task;
    }

    private Task EnqueueOperationAsync(Action operation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_taskQueue.Writer.TryWrite(() =>
        {
            try { operation(); tcs.SetResult(); }
            catch (Exception ex) { tcs.SetException(ex); }
        }))
        {
            tcs.SetException(new InvalidOperationException("Storage queue is closed."));
        }
        return tcs.Task;
    }

    #region Lock

    private void EnterWriteLock() => _fLock.LockExclusive();

    private void ExitWriteLock() => _fLock.Unlock();

    #endregion

    #region Internal File Operations


    private void SyncDataInternal()
    {
        long currentFileLength = DataFileStream.Length;
        if (currentFileLength == _lastSyncedPosition) return;
        if (currentFileLength < _lastSyncedPosition)
        {
            // 文件被截断，必须重建索引
            // 通常可能是因为被其他实例压缩或重写
            _memoryIndex.Clear();
            _lastSyncedPosition = 0;
        }

        DataFileStream.Seek(_lastSyncedPosition, SeekOrigin.Begin);

        using var reader = new BinaryReader(DataFileStream, Encoding.UTF8, leaveOpen: true);

        while (DataFileStream.Position < currentFileLength)
        {
            long recordStartPos = DataFileStream.Position;

            try
            {
                int ret;
                // 读取并校验，获取 Payload 数据块
                if ((ret = ReadRecordPayload(reader, out var payload)) != RRP_OK)
                {
                    LogError($"RRP error at {recordStartPos}: {ret}");
                    // 读取失败，可能是文件被截断，停止同步
                    break;
                }

                // 解析 Payload 来更新索引
                // Payload 结构: [Op 1B] + [Key (VarInt+Bytes)] + [Value (VarInt+Bytes)]?

                // 计算 Payload 在文件中的绝对起始位置
                // 记录开始位置 + Header(8 bytes)
                long payloadFileStartOffset = recordStartPos + HEADER_SIZE;

                var (OpCode, Key, Pointer) = ParsePayloadAndIndex(payload, payloadFileStartOffset);

                switch (OpCode)
                {
                    case OP_DEL:
                        _memoryIndex.TryRemove(Key, out _);
                        break;
                    case OP_SET:
                        _memoryIndex[Key] = Pointer;
                        break;
                    default:
                        break;
                }

                _lastSyncedPosition = DataFileStream.Position;
            }
            catch (Exception ex)
            {
                LogError($"Sync error at {recordStartPos}: {ex.Message}");
                break;
            }
        }
    }


    #endregion

    #region Static Helpers

    /// <summary>
    /// 解析 Payload 
    /// </summary>
    /// <param name="payload"></param>
    /// <param name="payloadFileStartOffset"></param>
    /// <returns></returns>
    private static (byte OpCode, string Key, ValuePointer Pointer) ParsePayloadAndIndex(byte[] payload, long payloadFileStartOffset)
    {
        using var ms = new MemoryStream(payload);
        using var reader = new BinaryReader(ms, Encoding.UTF8);

        byte op = reader.ReadByte();
        string key = reader.ReadString();

        if (op == OP_SET)
        {
            // BinaryReader.ReadString() 读取了 Key
            // 现在 Stream 的位置就是 Value 的起始位置（包含长度前缀）
            // 我们需要计算出这个位置相对于文件的绝对偏移量
            long valueOffsetInPayload = ms.Position;
            long absoluteValueOffset = payloadFileStartOffset + valueOffsetInPayload;

            // _memoryIndex[key] = new ValuePointer(absoluteValueOffset);
            return (op, key, new ValuePointer(absoluteValueOffset));
        }

        // _memoryIndex.TryRemove(key, out _);
        return (op, key, ValuePointer.Null);
    }

    /// <summary>
    /// 写入记录并返回该记录 Payload 中 Value 的绝对文件偏移量
    /// </summary>
    private static ValuePointer? WriteRecord(Stream stream, byte op, string key, string? value)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8);

        writer.Write(op);
        writer.Write(key);

        long valueOffsetInPayload = -1;
        if (op == OP_SET && value != null)
        {
            valueOffsetInPayload = ms.Position; // 记录 Value 在 Payload 中的相对位置
            writer.Write(value);
        }

        byte[] payload = ms.ToArray();

        // 写入物理文件
        long recordStartPos = stream.Position;
        using var diskWriter = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        diskWriter.Write(payload.Length);
        diskWriter.Write(System.IO.Hashing.Crc32.Hash(payload));
        diskWriter.Write(payload);

        if (valueOffsetInPayload != -1)
        {
            // Header Size = 4 (Length) + 4 (CRC) = 8
            long absoluteValueOffset = recordStartPos + HEADER_SIZE + valueOffsetInPayload;
            return new ValuePointer(absoluteValueOffset);
        }

        return null;
    }

    public const int RRP_OK = 0;
    public const int RRP_E_INVALID_LENGTH = -1;
    public const int RRP_E_CRC_MISMATCH = -2;
    public const int RRP_EOF = -3;
    public const int RRP_E_PAYLOAD_LENGTH_MISMATCH = -4;
    public const int RRP_E_UNKNOWN = -99;

    /// <summary>
    /// 读取记录并校验 CRC，返回原始 Payload 数据
    /// </summary>
    private static int ReadRecordPayload(BinaryReader reader, out byte[] payload)
    {
        payload = [];
        int length;
        try { length = reader.ReadInt32(); } catch (EndOfStreamException) { return RRP_EOF; }

        if (length <= 0 || length > MAX_RECORD_PAYLOAD_SIZE)
        {
            return RRP_E_INVALID_LENGTH;
        }

        uint storedCrc;
        try
        {
            storedCrc = reader.ReadUInt32();
            payload = reader.ReadBytes(length);
        }
        catch (EndOfStreamException) { return RRP_EOF; }

        if (payload.Length != length) return RRP_E_PAYLOAD_LENGTH_MISMATCH;

        var actualCrc = System.IO.Hashing.Crc32.Hash(payload);
        if (BitConverter.ToUInt32(actualCrc) != storedCrc)
        {
            return RRP_E_CRC_MISMATCH;
        }

        return RRP_OK;
    }

    /// <summary>
    /// 从文件指定位置读取 Value 字符串
    /// </summary>
    private static string? ReadValueFromFile(Stream dataStream, ValuePointer pointer)
    {
        // 保存当前位置
        long originalPos = dataStream.Position;
        try
        {
            dataStream.Seek(pointer.Offset, SeekOrigin.Begin);
            using var reader = new BinaryReader(dataStream, Encoding.UTF8, leaveOpen: true);
            return reader.ReadString();
        }
        catch (Exception ex)
        {
            LogError($"Failed to read value at {pointer.Offset}: {ex.Message}");
            return null;
        }
        finally
        {
            // 恢复位置（虽然在 Actor 模型单线程中通常不是必须的，因为每次操作都会 Seek，但为了保险起见）
            dataStream.Seek(originalPos, SeekOrigin.Begin);
        }
    }

    private static void LogError(string message)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [SharedKv] {message}");
    }

    #endregion

    #region Public APIs

    public override Task SetAsync(string key, string json)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return EnqueueOperationAsync(() =>
        {
            EnterWriteLock();
            try
            {
                SyncDataInternal();
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed");

                DataFileStream.Seek(0, SeekOrigin.End);

                // 写入并获取新的指针
                var ptr = WriteRecord(DataFileStream, OP_SET, key, json);
                DataFileStream.Flush();

                if (ptr.HasValue)
                {
                    _memoryIndex[key] = ptr.Value;
                }

                _lastSyncedPosition = DataFileStream.Position;
            }
            finally { ExitWriteLock(); }
        });
    }

    public override Task SetBatchAsync(ICollection<KeyValuePair<string, string>> data)
    {
        return EnqueueOperationAsync(() =>
        {
            EnterWriteLock();
            try
            {
                SyncDataInternal();
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed");

                DataFileStream.Seek(0, SeekOrigin.End);
                foreach (var kvp in data)
                {
                    var ptr = WriteRecord(DataFileStream, OP_SET, kvp.Key, kvp.Value);
                    if (ptr.HasValue)
                    {
                        _memoryIndex[kvp.Key] = ptr.Value;
                    }
                }
                DataFileStream.Flush();
                _lastSyncedPosition = DataFileStream.Position;
            }
            finally { ExitWriteLock(); }
        });
    }

    public override Task RemoveAsync(string key)
    {
        return EnqueueOperationAsync(() =>
        {
            if (!_memoryIndex.ContainsKey(key)) return;
            EnterWriteLock();
            try
            {
                SyncDataInternal();
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed");

                DataFileStream.Seek(0, SeekOrigin.End);
                WriteRecord(DataFileStream, OP_DEL, key, null);
                DataFileStream.Flush();

                _memoryIndex.TryRemove(key, out _);
                _lastSyncedPosition = DataFileStream.Position;
            }
            finally { ExitWriteLock(); }
        });
    }

    public override Task ClearAsync()
    {
        return EnqueueOperationAsync(() =>
        {
            EnterWriteLock();
            try
            {
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed");
                DataFileStream.SetLength(0);
                DataFileStream.Flush();
                _memoryIndex.Clear();
                _lastSyncedPosition = 0;
            }
            finally { ExitWriteLock(); }
        });
    }

    public override Task<string?> GetAsync(string key)
    {
        return EnqueueOperationAsync(() =>
        {
            try { SyncDataInternal(); } catch { }

            if (_memoryIndex.TryGetValue(key, out var ptr))
            {
                // 现在需要从文件读取
                return ReadValueFromFile(DataFileStream, ptr);
            }
            return null;
        });
    }

    public override Task<ICollection<KeyValuePair<string, string>>> GetBatchAsync(ICollection<string> keys)
    {
        return EnqueueOperationAsync<ICollection<KeyValuePair<string, string>>>(() =>
        {
            try { SyncDataInternal(); } catch { }
            var list = new List<KeyValuePair<string, string>>();
            foreach (var k in keys)
            {
                if (_memoryIndex.TryGetValue(k, out var ptr))
                {
                    var val = ReadValueFromFile(DataFileStream, ptr);
                    if (val != null)
                    {
                        list.Add(new(k, val));
                    }
                }
            }
            return list;
        });
    }

    public override Task<bool> HasKeyAsync(string key)
    {
        return EnqueueOperationAsync(() =>
        {
            try { SyncDataInternal(); } catch { }
            return _memoryIndex.ContainsKey(key);
        });
    }

    public override Task<ICollection<string>> GetKeysAsync()
    {
        return EnqueueOperationAsync<ICollection<string>>(() =>
        {
            try { SyncDataInternal(); } catch { }
            return [.. _memoryIndex.Keys];
        });
    }

    #endregion

    public async Task Compact()
    {
        await EnqueueOperationAsync(async () =>
        {
            bool lockTaken = false;
            try
            {
                if (DataFileStream is null) throw new InvalidOperationException("Stream closed");

                EnterWriteLock();
                lockTaken = true;
                SyncDataInternal();

                var tempPath = Path.GetTempFileName();
                if (File.Exists(tempPath)) File.Delete(tempPath);

                using var tempFs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, bufferSize: 4096, FileOptions.DeleteOnClose);

                // 遍历索引，从旧文件中读取出实际数据，写入到新文件
                CopyToInternal(tempFs);
                await tempFs.FlushAsync();

                tempFs.Seek(0, SeekOrigin.Begin);
                DataFileStream.Seek(0, SeekOrigin.Begin);
                DataFileStream.SetLength(0); // 清空旧文件

                await tempFs.CopyToAsync(DataFileStream);
                _lastSyncedPosition = 0; // 重置同步位置
                SyncDataInternal();
            }
            catch (Exception ex)
            {
                LogError($"Compact failed: {ex}");
            }
            finally
            {
                if (lockTaken) ExitWriteLock();
            }
        });
    }

    private void CopyToInternal(Stream tempFs)
    {
        foreach (var kvp in _memoryIndex)
        {
            var key = kvp.Key;
            var ptr = kvp.Value;
            var value = ReadValueFromFile(DataFileStream, ptr);

            if (value != null)
            {
                WriteRecord(tempFs, OP_SET, key, value);
            }
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _taskQueue.Writer.TryComplete();
                _cts.Cancel();
                try { _workerTask.Wait(TimeSpan.FromSeconds(2)); } catch { }
                _fLock.Dispose();
                _cts.Dispose();
                _dfs.Dispose();
                _lfs.Dispose();
                try { File.Delete(_lfs.Name); } catch { }
            }
            _disposed = true;
            GC.Collect();
        }
    }

    public async Task<(long Total, long Valid)> CheckFileValidityAsync(ICollection<(string msg, long ptr, int err)> errorList)
    {
        return await EnqueueOperationAsync(() =>
        {
            EnterWriteLock();
            errorList.Clear();
            var originalPtr = DataFileStream.Position;
            long counter = 0;
            var keys = new HashSet<string>();

            try
            {
                DataFileStream.Seek(0, SeekOrigin.Begin);
                using var reader = new BinaryReader(DataFileStream, Encoding.UTF8, leaveOpen: true);

                long ptr = reader.BaseStream.Position;
                while (true)
                {
                    int ret; ptr = reader.BaseStream.Position;
                    // 读取并校验，获取 Payload 数据块
                    if ((ret = ReadRecordPayload(reader, out var payload)) != RRP_OK)
                    {
                        if (ret == RRP_EOF)
                        {
                            // 正常结束
                            break;
                        }
                        else
                        {
                            errorList.Add(("RRP-ERR", ptr, ret));
                        }
                    }
                    else
                    {
                        var (OpCode, Key, _) = ParsePayloadAndIndex(payload, payloadFileStartOffset: 0); // 我们只需要 Key，因此 Offset 传入 0 即可

                        if (OpCode == OP_SET)
                            keys.Add(Key);
                        else if (OpCode == OP_DEL)
                            keys.Remove(Key);

                        ++counter;
                    }
                }

            }
            catch (Exception ex)
            {
                errorList.Add(($"EXCEPTION: {ex.Message}", DataFileStream.Position, RRP_E_UNKNOWN));
            }
            finally
            {
                DataFileStream.Position = originalPtr;
                ExitWriteLock();
            }

            return (counter, keys.Count);
        });
    }

    public override void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~SharedLogKvStorage() => Dispose(false);
}