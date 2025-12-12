namespace LocalStorage;

public abstract class KeyValueStorage: IDisposable
{
    public abstract string Name { get; }
    public abstract Task SetAsync(string key, string data);
    public abstract Task SetBatchAsync(ICollection<KeyValuePair<string, string>> data);
    public abstract Task<string?> GetAsync(string key);
    public abstract Task<ICollection<KeyValuePair<string, string>>> GetBatchAsync(ICollection<string> keys);
    public abstract Task RemoveAsync(string key);
    public abstract Task ClearAsync();
    public abstract Task<bool> HasKeyAsync(string key);
    public abstract Task<ICollection<string>> GetKeysAsync();

    public abstract void Dispose();

    public abstract bool IsDisposed { get; }

    public virtual string? Get(string key)
    {
        return GetAsync(key).GetAwaiter().GetResult();
    }

    public virtual ICollection<KeyValuePair<string, string>> GetBatch(ICollection<string> keys)
    {
        return GetBatchAsync(keys).GetAwaiter().GetResult();
    }

    public virtual void Set(string key, string data)
    {
        SetAsync(key, data).GetAwaiter().GetResult();
    }

    public virtual void SetBatch(ICollection<KeyValuePair<string, string>> data)
    {
        SetBatchAsync(data).GetAwaiter().GetResult();
    }

    public virtual void Clear()
    {
        ClearAsync().GetAwaiter().GetResult();
    }

    public virtual void Remove(string key)
    {
        RemoveAsync(key).GetAwaiter().GetResult();
    }

    public virtual bool HasKey(string key)
    {
        return HasKeyAsync(key).GetAwaiter().GetResult();
    }

    public virtual ICollection<string> GetKeys()
    {
        return GetKeysAsync().GetAwaiter().GetResult();
    }
}
