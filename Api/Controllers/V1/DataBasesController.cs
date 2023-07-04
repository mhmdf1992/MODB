using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using MODB.Api.Attributes;
using MODB.FlatFileDB;
using MODB.Api.DTOs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace MODB.Api.Controllers.V1
{
    [ApiKey]
    [ApiController]
    [Route("api/v1/[controller]")]
    public class DataBasesController : ControllerBase
    {
        readonly char[] KEY_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-".ToArray();
        readonly ConcurrentDictionary<string, ConcurrentDictionary<string, FlatFileKeyValDB>> _dbs;
        readonly Settings _settings;
        public DataBasesController(ConcurrentDictionary<string, ConcurrentDictionary<string, FlatFileKeyValDB>> dbs, Settings settings)
        {
            _dbs = dbs;
            _settings = settings;
        }

        [HttpGet]
        [ProducesResponseType(typeof(IEnumerable<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetDBsAsync()
        {
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            var res = Utilities.StopWatch(() => _dbs[apikey].Keys);
            Response.Headers.Add("processing-time", res.ProcessingTime);
            return await Task.FromResult(Ok(res.Result));
        }

        [HttpGet("{name}")]
        [ProducesResponseType(typeof(DBInformation), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetDBAsync([FromRoute] string name)
        {
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() =>{
                        var clientsDB = _dbs[apikey];
                        if(!clientsDB.ContainsKey(name))
                            throw new Exceptions.KeyNotFoundException(name);
                        return new DBInformation(){Name = name, Size = clientsDB[name].Size};
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpPost]
        [ProducesResponseType(typeof(OkResult), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> CreateDBAsync([FromQuery] CreateDBQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        ValidateKey(obj.Name);
                        var clientDBs = _dbs[apikey];
                        if(clientDBs.ContainsKey(obj.Name))
                            throw new Exceptions.UniqueKeyConstraintException(obj.Name);
                        _dbs[apikey].TryAdd(obj.Name, new FlatFileKeyValDB(System.IO.Path.Combine(_settings.Path.Concat(new string[]{apikey, obj.Name}).ToArray())));
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok());
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.UniqueKeyConstraintException ex){
                throw new Exceptions.ApplicationValidationErrorException(new ArgumentException(ex.Message, paramName: nameof(obj.Name)), HttpContext.TraceIdentifier);
            }
        }

        [HttpPost("{db}/Keys")]
        [ProducesResponseType(typeof(OkResult), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> SetKeyAsync([FromRoute] string db, [FromQuery] SetKeyQueryParams obj, [FromBody] Stream stream){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        _dbs[apikey][db].Set(obj.Key, stream, obj.Tags, obj.TimeStamp);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok());
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Keys")]
        [ProducesResponseType(typeof(PagedList<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetKeysAsync([FromRoute] string db, [FromQuery] GetPagedListQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].GetKeys(obj.Page, obj.PageSize);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Values")]
        [ProducesResponseType(typeof(PagedList<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetValuesAsync([FromRoute] string db, [FromQuery] GetFilteredPagedListQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].Get(obj.Tags, obj.From, obj.To, obj.OrderByKeyAsc, obj.OrderByKeyDesc, obj.OrderByTimeStampAsc, obj.OrderByTimeStampDesc, obj.Page, obj.PageSize);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Values/Detailed")]
        [ProducesResponseType(typeof(PagedList<MODBRecord>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetValuesDetailedAsync([FromRoute] string db, [FromQuery] GetFilteredPagedListQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].GetDetailed(obj.Tags, obj.From, obj.To, obj.OrderByKeyAsc, obj.OrderByKeyDesc, obj.OrderByTimeStampAsc, obj.OrderByTimeStampDesc, obj.Page, obj.PageSize);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Keys/{key}")]
        [ProducesResponseType(typeof(string), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetKeyAsync([FromRoute] string db, [FromRoute] string key){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].Get(key);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(FlatFileDB.Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Keys/{key}/Detailed")]
        [ProducesResponseType(typeof(MODBRecord), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetKeyDetailedAsync([FromRoute] string db, [FromRoute] string key){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].GetDetailed(key);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(FlatFileDB.Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Keys/{key}/exists")]
        [ProducesResponseType(typeof(string), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> KeyExistsAsync([FromRoute] string db, [FromRoute] string key){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].Exists(key);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(FlatFileDB.Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpDelete("{db}/Keys/{key}")]
        [ProducesResponseType(typeof(OkResult), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> DeleteKeyAsync([FromRoute] string db, [FromRoute] string key){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        _dbs[apikey][db].Delete(key);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok());
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        [HttpGet("{db}/Tags")]
        [ProducesResponseType(typeof(PagedList<string>), 200)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ValidationError), 400)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 404)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 401)]
        [ProducesResponseType(typeof(ConsistentApiResponseErrors.ConsistentErrors.ExceptionError), 500)]
        public async Task<IActionResult> GetTagsAsync([FromRoute] string db, [FromQuery] GetPagedListQueryParams obj){
            Request.Headers.TryGetValue("ApiKey", out var apikey);
            try{
                var res = Utilities.StopWatch(() => {
                        var clientDBs = _dbs[apikey];
                        if(!clientDBs.ContainsKey(db))
                            throw new Exceptions.KeyNotFoundException(db);
                        return _dbs[apikey][db].GetTags(obj.Page, obj.PageSize);
                    });
                Response.Headers.Add("processing-time", res.ProcessingTime);
                return await Task.FromResult(Ok(res.Result));
            }catch(ArgumentException ex){
                throw new Exceptions.ApplicationValidationErrorException(ex, HttpContext.TraceIdentifier);
            }
            catch(Exceptions.KeyNotFoundException ex){
                throw new Exceptions.ApplicationErrorException((int)System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.NotFound.ToString(), ex.Message);
            }
        }

        bool ValidateKey(string key) => string.IsNullOrEmpty(key) || key.Any(x => !KEY_ALLOWED_CHARS.Contains(x)) ? throw new ArgumentException($"{key} is not a valid key. keys must match ^[a-zA-Z0-9_.-]+$", nameof(key)) : true;
    }
}
