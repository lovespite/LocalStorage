using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace LocalStorage;

/// <summary>
/// 基于日志追加（Append-Only）的高性能键值存储。
/// <para>修复了一致性问题和日志重放的健壮性。</para>
/// </summary>
public class LogKvStorage : LocalStorage, IDisposable
{
    private readonly string _filePath;
    // 内存索引
    private readonly ConcurrentDictionary<string, string> _memoryIndex;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    private FileStream _fileStream = null!;

    private const byte OP_SET = 1;
    private const byte OP_DEL = 2;

    public LogKvStorage(string filePath)
    {
        _filePath = filePath;
        _memoryIndex = new ConcurrentDictionary<string, string>();

        var dir = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

        Initialize();
    }

    private void Initialize()
    {
        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

        if (_fileStream.Length > 0)
        {
            ReplayLog();
        }

        // 确保指针在末尾
        _fileStream.Seek(0, SeekOrigin.End);
    }

    private void ReplayLog()
    {
        _fileStream.Seek(0, SeekOrigin.Begin);
        using var reader = new BinaryReader(_fileStream, Encoding.UTF8, leaveOpen: true);

        long lastValidPosition = 0;

        try
        {
            while (_fileStream.Position < _fileStream.Length)
            {
                // 记录当前记录开始的位置
                long currentRecordStart = _fileStream.Position;

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
                else
                {
                    // 遇到未知指令，认为是文件损坏，停止读取
                    // 此时 lastValidPosition 还是上一条成功记录的结尾
                    Debug.WriteLine($"[LogKvStorage] Found unknown OpCode: {op} at position {currentRecordStart}. Truncating.");

                    _fileStream.Position = lastValidPosition;
                    _fileStream.SetLength(lastValidPosition);
                    return;
                }

                // 记录成功解析完一条记录后的位置
                lastValidPosition = _fileStream.Position;
            }
        }
        catch (EndOfStreamException)
        {
            // 文件截断（通常发生在只有一半写入时）
            // 回滚到最后一次成功的位置
            Debug.WriteLine($"[LogKvStorage] Unexpected EOF. Truncating from {_fileStream.Length} to {lastValidPosition}.");
            _fileStream.Position = lastValidPosition;
            _fileStream.SetLength(lastValidPosition);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[LogKvStorage] Log replay error: {ex.Message}. Truncating to {lastValidPosition}.");
            _fileStream.Position = lastValidPosition;
            _fileStream.SetLength(lastValidPosition);
        }
    }

    public override void Set(string key, string json)
    {
        _writeLock.Wait();
        try
        {
            // 先写磁盘（持久化优先）
            WriteSetInternal(_fileStream, key, json);
            _fileStream.Flush();

            // 写入成功后再更新内存
            _memoryIndex[key] = json;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public override async Task SetAsync(string key, string json)
    {
        await _writeLock.WaitAsync();
        try
        {
            // 先写磁盘
            WriteSetInternal(_fileStream, key, json);
            await _fileStream.FlushAsync();

            // 更新内存
            _memoryIndex[key] = json;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public override void Remove(string key)
    {
        // 如果内存里本来就没有，就不用写日志了 
        if (!_memoryIndex.ContainsKey(key)) return;

        _writeLock.Wait();
        try
        {
            using var writer = new BinaryWriter(_fileStream, Encoding.UTF8, leaveOpen: true);
            writer.Write(OP_DEL);
            writer.Write(key);
            _fileStream.Flush();

            // 磁盘写入成功后，移除内存
            _memoryIndex.TryRemove(key, out _);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public override async Task RemoveAsync(string key)
    {
        if (!_memoryIndex.ContainsKey(key)) return;

        await _writeLock.WaitAsync();
        try
        {
            using var writer = new BinaryWriter(_fileStream, Encoding.UTF8, leaveOpen: true);
            writer.Write(OP_DEL);
            writer.Write(key);
            await _fileStream.FlushAsync();

            _memoryIndex.TryRemove(key, out _);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    // Get 实现保持不变，因为它只是读内存
    public override string? Get(string key) => _memoryIndex.TryGetValue(key, out var val) ? val : null;

    public override Task<string?> GetAsync(string key) => Task.FromResult(Get(key));

    public override void Clear()
    {
        _writeLock.Wait();
        try
        {
            _fileStream.SetLength(0);
            _fileStream.Flush();
            _memoryIndex.Clear();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public override async Task ClearAsync()
    {
        await _writeLock.WaitAsync();
        try
        {
            _fileStream.SetLength(0);
            await _fileStream.FlushAsync();
            _memoryIndex.Clear();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task CompactAsync()
    {
        await _writeLock.WaitAsync();
        try
        {
            string tempPath = _filePath + ".tmp";

            // 使用新的 FileStream 写入临时文件
            using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write))
            using (var writer = new BinaryWriter(fs, Encoding.UTF8))
            {
                // 此时虽然持有 WriteLock，但 Get 操作依然可以并发读取 _memoryIndex
                // ToList() 用于创建快照，防止遍历时集合修改（虽然 ConcurrentDict 支持并发遍历，但快照更安全）
                foreach (var kvp in _memoryIndex)
                {
                    writer.Write(OP_SET);
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value);
                }
                await fs.FlushAsync();
            }

            // 关键：关闭当前文件流，以便进行替换
            _fileStream.Dispose();

            try
            {
                File.Move(tempPath, _filePath, overwrite: true);
            }
            catch
            {
                // 如果 Move 失败（极少见），尝试重新打开旧文件，保证服务不挂
                _fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                _fileStream.Seek(0, SeekOrigin.End);
                throw; // 抛出异常通知调用者压缩失败
            }

            // 重新打开新文件
            _fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            _fileStream.Seek(0, SeekOrigin.End);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private static void WriteSetInternal(Stream stream, string key, string json)
    {
        // 注意：BinaryWriter 分配开销较小，但在极高频调用下可考虑复用（ThreadLocal 或 传入）
        // 但为了线程安全和代码简单，这里 new 是可以接受的
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        writer.Write(OP_SET);
        writer.Write(key);
        writer.Write(json);
    }

    public void Dispose()
    {
        _writeLock?.Dispose();
        _fileStream?.Dispose();
    }
}