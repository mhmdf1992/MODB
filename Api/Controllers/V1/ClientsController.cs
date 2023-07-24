using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using MODB.Api.Attributes;
using MODB.FlatFileDB;
using MODB.Api.DTOs;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;

namespace MODB.Api.Controllers.V1
{
    [AdminApiKey]
    [ApiController]
    [Route("api/v1/[controller]")]
    public class ClientsController : ControllerBase
    {
        
        readonly Settings _settings;
        readonly IKeyValDB _clientsDB;
        readonly ConcurrentDictionary<string, ConcurrentDictionary<string, FlatFileKeyValDB>> _clientsDBs;
        public ClientsController(Settings settings, IKeyValDB clientsDB, ConcurrentDictionary<string, ConcurrentDictionary<string, FlatFileKeyValDB>> clientsDBs)
        {
            _settings = settings;
            _clientsDB = clientsDB;
            _clientsDBs = clientsDBs;
        }

        [HttpGet]
        [ProducesResponseType(typeof(PagedList<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetClientsOrderedAsync([FromQuery] GetOrderedQueryParams obj){
            var res = Utilities.StopWatch(() => _clientsDB.GetOrdered(obj.Tags, obj.From, obj.To, obj.OrderByKeyAsc, obj.OrderByKeyDesc, obj.OrderByTimeStampAsc, obj.OrderByTimeStampDesc, obj.Page, obj.PageSize));
            Response.Headers.Add("processing-time", res.ProcessingTime);
            return await Task.FromResult(Ok(res.Result));
        }   

        [HttpGet("{key}")]
        [ProducesResponseType(typeof(string), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetClientAsync([FromRoute] string key)
        {
            try{
                var res = Utilities.StopWatch(() => _clientsDB.Get(key));
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(FlatFileDB.Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }
        
        [HttpPost]
        [ProducesResponseType(typeof(OkResult), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> SetClientAsync([FromQuery] SetKeyQueryParams obj, [FromBody] Stream stream){
            try{
                var res = Utilities.StopWatch(() => {
                    _clientsDB.Set(obj.Key, stream, obj.Tags, obj.TimeStamp);
                    _clientsDBs.TryAdd(obj.Key, new ConcurrentDictionary<string, FlatFileKeyValDB>());});
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok());
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            
        }

        [HttpDelete("{key}")]
        [ProducesResponseType(typeof(OkResult), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> DeleteClientAsync([FromRoute] string key)
        {
            try{
                var res = Utilities.StopWatch(() => {
                    _clientsDB.Delete(key);
                    _clientsDBs.TryRemove(key, out ConcurrentDictionary<string, MODB.FlatFileDB.FlatFileKeyValDB> val);
                });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok());
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(FlatFileDB.Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("Tags")]
        [ProducesResponseType(typeof(PagedList<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetTagsAsync([FromQuery] GetTagsOrderedQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => _clientsDB.GetTagsOrdered(obj.Text, obj.OrderAsc, obj.OrderDesc, obj.Page, obj.PageSize));
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }
    }
}
