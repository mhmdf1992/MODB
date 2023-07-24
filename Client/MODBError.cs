using System.Collections.Generic;

namespace MODB.Client{
    public class MODBError{
        public int StatusCode {get; set;}
        public string StatusMessage {get; set;}
        public string ErrorMessage {get; set;}
        public IEnumerable<Error> Errors {get; set;}
    }

    public class Error
    {
        public object Code { get; set; }
        public string Field { get; set; }
        public object AttemptedValue { get; set; }
        public string Message { get; set; }
        public string HelpURL { get; set; }
    }
}