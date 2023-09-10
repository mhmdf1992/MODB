using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MODB.CHTTPClient;
using MODB.Client.DTOs;

namespace MODB.Client{
    public class MODBClient : IMODBClient, IDisposable
    {
        HTTPClient _httpClient;
        string _baseUrl;
        string Endpoint(string resource) => $"{_baseUrl}/{resource}";
        public MODBClient(string host, string apikey, string version = "v1"){
            if(string.IsNullOrEmpty(host))
                throw new ArgumentException(paramName: "host", message: "Invalid host, can not be null or empty");
            if(string.IsNullOrEmpty(apikey))
                throw new ArgumentException(paramName: "apikey", message: "Invalid apikey, can not be null or empty");
            _baseUrl = $"api/{version}";
            _httpClient = new HTTPClient(host, new Dictionary<string, string>(){{"ApiKey", apikey} });
        }
        public async Task CreateDBAsync(string name, int? manifests = 10, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(name))
                throw new ArgumentException(paramName: "name", message: "Database name, can not be null or empty");
            var queryStringParams = new Dictionary<string, string>(){
                {"name", name}
            };
            if(manifests != null)
                queryStringParams.Add("manifests", $"{manifests}");
            try{
                await _httpClient.PostAsync(
                    endpoint: Endpoint("databases"),
                    action: stream => {},
                    body: null,
                    queryStringParams: queryStringParams,
                    cancellationToken: cs
                );
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task DeleteAsync(string db, string key, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(string.IsNullOrEmpty(key))
                throw new ArgumentException(paramName: "key", message: "key, can not be null or empty");
            try{
                await _httpClient.DeleteAsync(
                    endpoint: Endpoint("databases/{0}/keys/{1}"),
                    action: stream => {}, 
                    cancellationToken: cs,
                    routeParams: new string[]{db, key});
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }

        public async Task<bool> ExistsAsync(string db, string key, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(string.IsNullOrEmpty(key))
                throw new ArgumentException(paramName: "key", message: "key, can not be null or empty");
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}/keys/{1}/exists"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<bool>>(stream).Result, 
                    cancellationToken: cs,
                    routeParams: new string[]{db, key});
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<T> GetAsync<T>(string db, string key, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(string.IsNullOrEmpty(key))
                throw new ArgumentException(paramName: "key", message: "key, can not be null or empty");
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}/keys/{1}"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<T>>(stream).Result, 
                    cancellationToken: cs,
                    routeParams: new string[]{db, key});
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<DBInformation> GetDBAsync(string name, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(name))
                throw new ArgumentException(paramName: "name", message: "Database name, can not be null or empty");
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<DBInformation>>(stream).Result, 
                    cancellationToken: cs,
                    routeParams: name);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<IEnumerable<string>> GetDBsAsync(CancellationToken cs = default)
        {
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<IEnumerable<string>>>(stream).Result, 
                    cancellationToken: cs);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<PagedList<string>> GetKeysAsync(string db, IEnumerable<string> tags = null, long? from = null, long? to = null, int page = 1, int pageSize = 10, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(page <= 0)
                throw new ArgumentException(paramName: "page", message: "Page, must be greater than 0");
            if(pageSize <= 0)
                throw new ArgumentException(paramName: "pageSize", message: "Page size, must be greater than 0");
            var queryStringParams = new Dictionary<string, string>(){
                {"page", $"{page}"},
                {"pageSize", $"{pageSize}"}
            };
            if(tags != null && tags.Any())
                queryStringParams.Add("tags", string.Join(',', tags));
            if(from != null) 
                queryStringParams.Add("from", $"{from}");
            if(to != null)
                queryStringParams.Add("to", $"{to}");
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}/keys"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<PagedList<string>>>(stream).Result, 
                    queryStringParams: queryStringParams,
                    cancellationToken: cs,
                    routeParams: db);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<PagedList<string>> GetTagsAsync(string db, string filter = null, int page = 1, int pageSize = 10, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(page <= 0)
                throw new ArgumentException(paramName: "page", message: "Page, must be greater than 0");
            if(pageSize <= 0)
                throw new ArgumentException(paramName: "pageSize", message: "Page size, must be greater than 0");
            
            var queryStringParams = new Dictionary<string, string>(){
                {"page", $"{page}"},
                {"pageSize", $"{pageSize}"}
            };

            if(!string.IsNullOrEmpty(filter))
                queryStringParams.Add("filter", filter);
            
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}/tags"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<PagedList<string>>>(stream).Result, 
                    queryStringParams: queryStringParams,
                    cancellationToken: cs,
                    routeParams: db);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task SetAsync(string db, string key, string value, IEnumerable<string> tags = null, long? timestamp = null, bool? createDb = true, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(string.IsNullOrEmpty(key))
                throw new ArgumentException(paramName: "key", message: "key, can not be null or empty");
            if(string.IsNullOrEmpty(value))
                throw new ArgumentException(paramName: "value", message: "value, can not be null or empty");

            var queryStringParams = new Dictionary<string, string>(){
                {"key", key}
            };
            if(tags != null && tags.Any())
                queryStringParams.Add("tags", string.Join(',', tags));
            if(timestamp != null) 
                queryStringParams.Add("timestamp", $"{timestamp}");
            if(createDb != null)
                queryStringParams.Add("createdb", $"{createDb}");
            try{
                await _httpClient.PostAsync(
                    endpoint: Endpoint("databases/{0}/keys"),
                    action: stream => {},
                    body: new System.Net.Http.StringContent(value), 
                    queryStringParams: queryStringParams,
                    cancellationToken: cs,
                    routeParams: db);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }

        public async Task<PagedList<T>> GetValuesAsync<T>(string db, IEnumerable<string> tags = null, long? from = null, long? to = null, int page = 1, int pageSize = 10, CancellationToken cs = default)
        {
            if(string.IsNullOrEmpty(db))
                throw new ArgumentException(paramName: "db", message: "Database name, can not be null or empty");
            if(page <= 0)
                throw new ArgumentException(paramName: "page", message: "Page, must be greater than 0");
            if(pageSize <= 0)
                throw new ArgumentException(paramName: "pageSize", message: "Page size, must be greater than 0");
            var queryStringParams = new Dictionary<string, string>(){
                {"page", $"{page}"},
                {"pageSize", $"{pageSize}"}
            };
            if(tags != null && tags.Any())
                queryStringParams.Add("tags", string.Join(',', tags));
            if(from != null) 
                queryStringParams.Add("from", $"{from}");
            if(to != null)
                queryStringParams.Add("to", $"{to}");
            try{
                return await _httpClient.GetAsync(
                    endpoint: Endpoint("databases/{0}/values"),
                    func: stream => JsonSerializer.Deserialize<MODBResponse<PagedList<T>>>(stream).Result, 
                    queryStringParams: queryStringParams,
                    cancellationToken: cs,
                    routeParams: db);
            }catch(CHTTPRequestFailedException ex){
                throw new MODBRequestFailedException(ex, JsonSerializer.Deserialize<MODBError>(ex.Response), ex.StatusCode, ex.StatusMessage);
            }
        }
    }
}