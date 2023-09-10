using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace MODB.Client.DTOs{
    public class MODBResponse<T>{
        [JsonPropertyName("result")]
        public T Result {get; set;}
    }

    public class MODBError{
        [JsonPropertyName("statusCode")]
        public int StatusCode {get; set;}
        [JsonPropertyName("statusMessage")]
        public string StatusMessage {get; set;}
        [JsonPropertyName("traceId")]
        public string TraceId {get; set;}
        [JsonPropertyName("errorMessage")]
        public string ErrorMessage {get; set;}
        [JsonPropertyName("errors")]
        public IEnumerable<Error> Errors {get; set;}
    }

    public class Error{
        [JsonPropertyName("code")]
        public object Code { get; set; }
        [JsonPropertyName("field")]
        public string Field { get; set; }
        [JsonPropertyName("attemptedValue")]
        public object AttemptedValue { get; set; }
        [JsonPropertyName("message")]
        public string Message { get; set; }
        [JsonPropertyName("helpURL")]
        public string HelpURL { get; set; }
    }

    public class PagedList<T>{
        [JsonPropertyName("page")]
        public int Page {get; set;}
        [JsonPropertyName("pageSize")]
        public int PageSize {get; set;}
        [JsonPropertyName("totalPages")]
        public int TotalPages {get; set;}
        [JsonPropertyName("totalItems")]
        public int TotalItems {get; set;}
        [JsonPropertyName("items")]
        public IEnumerable<T> Items {get; set;}
    }
}