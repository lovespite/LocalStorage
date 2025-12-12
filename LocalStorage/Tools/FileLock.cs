using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using System.Diagnostics;

namespace LocalStorage.Tools;

public partial class FileLock : IDisposable
{
    private readonly SafeFileHandle _handle;

    // --- 配置字段 (Config) ---
    // 这些值用于下一次锁定尝试
    private long _configOffset = long.MaxValue - 1;
    private long _configSize = 1;

    // --- 状态字段 (State) ---
    // 这些值记录当前实际锁定的区域，仅在 _isLocked = true 时有效
    private bool _isLocked;
    private long _actualLockedOffset;
    private long _actualLockedSize;

    private readonly System.Threading.Lock @lock = new();

    /// <summary>
    /// Gets or sets the starting byte offset of the lock.
    /// </summary>
    public long LockOffset
    {
        get => _configOffset;
        set
        {
            if (_isLocked) throw new InvalidOperationException("Cannot change LockOffset while the lock is held.");
            _configOffset = value;
        }
    }

    /// <summary>
    /// Gets or sets the length of the byte range to be locked.
    /// </summary>
    public long LockSize
    {
        get => _configSize;
        set
        {
            if (_isLocked) throw new InvalidOperationException("Cannot change LockSize while the lock is held.");
            _configSize = value;
        }
    }

    public FileLock AtRange(long start, long len)
    {
        LockOffset = start;
        LockSize = len;
        return this;
    }

    public bool IsLocked
    {
        get { lock (@lock) { return _isLocked; } }
    }

    public string Name { get; }

    public FileLock(SafeFileHandle handle, string name)
    {
        ArgumentNullException.ThrowIfNull(handle);
        if (handle.IsInvalid || handle.IsClosed)
            throw new ArgumentException("Handle is invalid or closed.", nameof(handle));

        _handle = handle;
        Name = name;
    }

    public FileLock(SafeFileHandle handle) : this(handle, Guid.NewGuid().ToString("N")) { }

    #region Native API

    [LibraryImport("kernel32.dll", EntryPoint = "LockFileEx", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool LockFileEx(IntPtr hFile, uint dwFlags, uint dwReserved, uint nNumberOfBytesToLockLow, uint nNumberOfBytesToLockHigh, ref OVERLAPPED lpOverlapped);

    [LibraryImport("kernel32.dll", EntryPoint = "UnlockFileEx", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool UnlockFileEx(IntPtr hFile, uint dwReserved, uint nNumberOfBytesToUnlockLow, uint nNumberOfBytesToUnlockHigh, ref OVERLAPPED lpOverlapped);

    [StructLayout(LayoutKind.Sequential)]
    private struct OVERLAPPED
    {
        public IntPtr Internal;
        public IntPtr InternalHigh;
        public uint Offset;
        public uint OffsetHigh;
        public IntPtr hEvent;
    }

    private const uint LOCKFILE_EXCLUSIVE_LOCK = 0x00000002;
    private const uint LOCKFILE_FAIL_IMMEDIATELY = 0x00000001;

    private const int ERROR_NOT_LOCKED = 158;
    private const int ERROR_LOCK_VIOLATION = 33;

    #endregion

    public void LockExclusive(TimeSpan timeout)
    {
        if (_isLocked) throw new InvalidOperationException("FileLock is already acquired.");
        lock (@lock)
        {
            if (_isLocked) throw new InvalidOperationException("FileLock is already acquired.");
            LockInternal(timeout);
        }
    }

    private void LockInternal(TimeSpan timeout)
    {
        ObjectDisposedException.ThrowIf(_handle.IsClosed || _handle.IsInvalid, _handle);

        // 1. 获取配置快照
        long targetOffset = _configOffset;
        long targetSize = _configSize;

        ArgumentOutOfRangeException.ThrowIfLessThan(targetOffset, 0, nameof(LockOffset));
        ArgumentOutOfRangeException.ThrowIfLessThan(targetSize, 1, nameof(LockSize));

        // 准备 Overlapped
        var overlapped = new OVERLAPPED
        {
            Offset = (uint)(targetOffset & 0xFFFFFFFF),
            OffsetHigh = (uint)(targetOffset >> 32),
            hEvent = IntPtr.Zero
        };

        uint sizeLow = (uint)(targetSize & 0xFFFFFFFF);
        uint sizeHigh = (uint)(targetSize >> 32);

        // 安全引用 Handle
        bool refAdded = false;
        try
        {
            _handle.DangerousAddRef(ref refAdded);
            IntPtr rawHandle = _handle.DangerousGetHandle();

            // 等待循环
            // 使用 SpinWait 结构虽然高效，但对于文件IO锁，如果超时时间较长
            var spinWait = new SpinWait();
            long startTicks = Stopwatch.GetTimestamp();

            while (true)
            {
                var ok = LockFileInternal(timeout, targetOffset, targetSize, ref overlapped, sizeLow, sizeHigh, rawHandle, startTicks);
                if (ok) return;
                spinWait.SpinOnce();
                if (spinWait.NextSpinWillYield) Thread.Sleep(1);
            }
        }
        finally
        {
            if (refAdded) _handle.DangerousRelease();
        }
    }

    private bool LockFileInternal(TimeSpan timeout, long targetOffset, long targetSize, ref OVERLAPPED overlapped, uint sizeLow, uint sizeHigh, nint rawHandle, long startTicks)
    {
        // 尝试立即锁定
        if (LockFileEx(rawHandle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, sizeLow, sizeHigh, ref overlapped))
        {
            // 成功：更新内部状态
            _isLocked = true;
            _actualLockedOffset = targetOffset;
            _actualLockedSize = targetSize;
            return true;
        }

        int error = Marshal.GetLastWin32Error();

        // 只有 "Lock Violation" (33) 才是需要重试的情况
        if (error != ERROR_LOCK_VIOLATION)
        {
            throw new System.ComponentModel.Win32Exception(error);
        }

        // 检查超时
        if (timeout != TimeSpan.Zero && Stopwatch.GetElapsedTime(startTicks) > timeout)
        {
            throw new TimeoutException($"Failed to acquire file lock at offset {targetOffset} within {timeout.TotalMilliseconds}ms.");
        }

        // 立即失败模式
        if (timeout == TimeSpan.Zero)
        {
            throw new TimeoutException("Failed to acquire file lock immediately.");
        }

        return false;
    }

    public void LockExclusive() => LockExclusive(TimeSpan.FromMilliseconds(int.MaxValue));

    public bool TryLockExclusiveImmediate()
    {
        try
        {
            LockExclusive(TimeSpan.Zero);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    public void Unlock()
    {
        if (!_isLocked) return;
        lock (@lock)
        {
            if (!_isLocked) return;

            // 使用 _actualLockedOffset (锁定时的快照)，确保解锁区域与锁定区域一致 
            var overlapped = new OVERLAPPED
            {
                Offset = (uint)(_actualLockedOffset & 0xFFFFFFFF),
                OffsetHigh = (uint)(_actualLockedOffset >> 32),
                hEvent = IntPtr.Zero
            };
            uint sizeLow = (uint)(_actualLockedSize & 0xFFFFFFFF);
            uint sizeHigh = (uint)(_actualLockedSize >> 32);

            // 2. 解锁
            bool refAdded = false;
            try
            {
                _handle.DangerousAddRef(ref refAdded);
                IntPtr rawHandle = _handle.DangerousGetHandle();

                if (!UnlockFileEx(rawHandle, 0, sizeLow, sizeHigh, ref overlapped))
                {
                    int error = Marshal.GetLastWin32Error();
                    if (error != ERROR_NOT_LOCKED)
                    {
                        throw new System.ComponentModel.Win32Exception(error);
                    }
                }
            }
            finally
            {
                // 状态重置
                _isLocked = false;
                _actualLockedOffset = 0;
                _actualLockedSize = 0;

                if (refAdded) _handle.DangerousRelease();
            }
        }
    }

    public void Dispose()
    {
        Unlock();
        GC.SuppressFinalize(this);
    }
}