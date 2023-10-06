using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace MODB.CHTTPClient{
    public class HTTPClient : IHTTPClient, IDisposable
    {
        private System.Net.Http.HttpClient _client;
        private string _host;
        private TimeSpan _timeout;
        private Dictionary<string, string> _headers;
        public string Host => _host;
        public Dictionary<string, string> Headers => _headers;

        public HTTPClient(string host, Dictionary<string, string> headers = null, TimeSpan? timeout = null){
            _host = host;
            _headers = headers ?? new Dictionary<string, string>();
            _timeout = timeout ?? TimeSpan.FromSeconds(90);
        }
        private System.Net.Http.HttpClient GetClientInstance(){
            if(_client == null){
                _client = new System.Net.Http.HttpClient();
                if(!string.IsNullOrEmpty(_host))
                    _client.BaseAddress = new System.Uri(_host);
                if(_headers != null && _headers.Any())
                    _headers.ToList().ForEach(header => _client.DefaultRequestHeaders.Add(header.Key, header.Value));
                _client.Timeout = _timeout;
            }
            return _client;
        } 

        public async Task<string> DeleteAsync(string endpoint, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().DeleteAsync(BuildEndpoint(endpoint, routeParams), cancellationToken)){
                var res = await response.Content.ReadAsStringAsync();
                try
                {
                    response.EnsureSuccessStatusCode();
                    return res;
                }catch (System.Net.Http.HttpRequestException ex)
                {
                    throw new CHTTPRequestFailedException(ex, res, (int)response.StatusCode, response.StatusCode.ToString());
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task DeleteAsync (string endpoint, Action<Stream> action, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().DeleteAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", cancellationToken)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   action(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<string> GetAsync(string endpoint, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().GetAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", cancellationToken)){
                var res = await response.Content.ReadAsStringAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return res;
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    throw new CHTTPRequestFailedException(ex, res, (int)response.StatusCode, response.StatusCode.ToString());
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<string> PostAsync(string endpoint, System.Net.Http.HttpContent body, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().PostAsync(BuildEndpoint(endpoint, routeParams), body, cancellationToken)){
                var res = await response.Content.ReadAsStringAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return res;
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    throw new CHTTPRequestFailedException(ex, res, (int)response.StatusCode, response.StatusCode.ToString());
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<string> PutAsync(string endpoint, System.Net.Http.HttpContent body, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().PutAsync(BuildEndpoint(endpoint, routeParams), body, cancellationToken)){
                var res = await response.Content.ReadAsStringAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return res;
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    throw new CHTTPRequestFailedException(ex, res, (int)response.StatusCode, response.StatusCode.ToString());
                }
                catch
                {
                    throw;
                }
            }
        }

        public void Dispose()
        {
            _client?.Dispose();
        }

        public string ConvertToQueryString(Dictionary<string,string> queryStringParams)
        {
            if(queryStringParams == null || !queryStringParams.Any())
                return "";
            return $"?{string.Join("&", queryStringParams.Select(parm => $"{parm.Key}={parm.Value}"))}";
        }

        public string BuildEndpoint(string endpoint, params string[] routeParams){
            if(routeParams == null || !routeParams.Any())
                return endpoint;
            return string.Format(endpoint, routeParams);
        }

        public async Task<T> GetAsync<T>(string endpoint, Func<Stream, T> func, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().GetAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", cancellationToken)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return func(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<T> PostAsync<T>(string endpoint, Func<Stream, T> func, HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().PostAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", body, cancellationToken)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return func(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task PostAsync(string endpoint, Action<Stream> action, HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().PostAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", body, cancellationToken)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   action(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<T> PutAsync<T>(string endpoint, Func<Stream, T> func, HttpContent body, Dictionary<string, string> queryStringParams = null, CancellationToken cancellationToken = default, params string[] routeParams)
        {
            using (var response = await GetClientInstance().PutAsync($"{BuildEndpoint(endpoint, routeParams)}{ConvertToQueryString(queryStringParams)}", body, cancellationToken)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return func(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public async Task<T> SendAsync<T>(HttpRequestMessage request, Func<Stream, T> func, CancellationToken cancellationToken = default)
        {
            using (var response = await GetClientInstance().SendAsync(request)){
                var res = await response.Content.ReadAsStreamAsync();
                try{
                   response.EnsureSuccessStatusCode();
                   return func(res);
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    using(StreamReader streamReader = new StreamReader(res)){
                        throw new CHTTPRequestFailedException(ex, await streamReader.ReadToEndAsync(), (int)response.StatusCode, response.StatusCode.ToString());
                    }
                }
                catch
                {
                    throw;
                }
            }
        }
    }
}