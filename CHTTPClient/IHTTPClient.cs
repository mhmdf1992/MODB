using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace MODB.CHTTPClient
{
    public interface IHTTPClient
    {
        Dictionary<string, string> Headers {get;}
        string Host {get;}
        Task<T> GetAsync<T>(string endpoint, System.Func<System.IO.Stream, T> func, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<string> GetAsync(string endpoint, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<T> PostAsync<T>(string endpoint, System.Func<System.IO.Stream, T> func, System.Net.Http.HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task PostAsync(string endpoint, System.Action<System.IO.Stream> action, System.Net.Http.HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<string> PostAsync(string endpoint, System.Net.Http.HttpContent body, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<string> PutAsync(string endpoint, System.Net.Http.HttpContent body, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<T> PutAsync<T>(string endpoint, System.Func<System.IO.Stream, T> func, System.Net.Http.HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<string> DeleteAsync(string endpoint, CancellationToken cancellationToken = default, params string[] routeParams);
        Task DeleteAsync (string endpoint, System.Action<System.IO.Stream> action, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams);
        Task<T> SendAsync<T>(HttpRequestMessage request, System.Func<System.IO.Stream, T> func, CancellationToken cancellationToken = default);
        void Dispose();
        string ConvertToQueryString(Dictionary<string,string> queryStringParams);
        string BuildEndpoint(string endpoint, params string[] routeParams);
    }
}