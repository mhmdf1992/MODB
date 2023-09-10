using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using MODB.Client.DTOs;
namespace MODB.Client{
    public interface IMODBClient{
        Task<IEnumerable<string>> GetDBsAsync(CancellationToken cs = default);
        Task<DBInformation> GetDBAsync(string name, CancellationToken cs = default);
        Task CreateDBAsync(string name, int? manifests = 10, CancellationToken cs = default);
        Task SetAsync(string db, string key, string value, IEnumerable<string> tags = null, long? timestamp = null, bool? createDb = true, CancellationToken cs = default);
        Task<T> GetAsync<T>(string db, string key, CancellationToken cs = default);
        Task<bool> ExistsAsync(string db, string key, CancellationToken cs = default);
        Task DeleteAsync(string db, string key, CancellationToken cs = default);
        Task<PagedList<string>>GetTagsAsync(string db, string filter = null, int page = 1, int pageSize = 10, CancellationToken  cs = default);
        Task<PagedList<string>> GetKeysAsync(string db, IEnumerable<string> tags = null, long? from = null, long? to = null, int page = 1, int pageSize = 10, CancellationToken cs = default);
        Task<PagedList<T>> GetValuesAsync<T>(string db, IEnumerable<string> tags = null, long? from = null, long? to = null, int page = 1, int pageSize = 10, CancellationToken cs = default);
    }
}