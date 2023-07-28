using System.Collections.Generic;
using System.Text.Json.Serialization;
namespace MODB.Client{
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

    public class Error
    {
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
}