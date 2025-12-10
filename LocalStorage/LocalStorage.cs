using System.Text.Json.Serialization.Metadata;

namespace LocalStorage;

public abstract class KeyValueStorage
{
    public abstract Task SetAsync(string key, string data);
    public abstract Task<string?> GetAsync(string key);
    public abstract Task RemoveAsync(string key);
    public abstract Task ClearAsync();

    public virtual string? Get(string key)
    {
        return GetAsync(key).GetAwaiter().GetResult();
    }

    public virtual void Set(string key, string data)
    {
        SetAsync(key, data).GetAwaiter().GetResult();
    }

    public virtual void Clear()
    {
        ClearAsync().GetAwaiter().GetResult();
    }

    public virtual void Remove(string key)
    {
        RemoveAsync(key).GetAwaiter().GetResult();
    }
}
