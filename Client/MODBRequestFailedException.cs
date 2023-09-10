using System;
using MODB.Client.DTOs;

namespace MODB.Client{
    public class MODBRequestFailedException : Exception{
        MODBError _response;
        public MODBError Response => _response;
        int _statusCode;
        public int StatusCode => _statusCode;
        string _statusMessage;
        public string StatusMessage => _statusMessage;
        public MODBRequestFailedException(Exception inner, MODBError response, int statusCode, string statusMessage): base("Status code does not indicate success", inner){
            _response = response;
            _statusCode = statusCode;
            _statusMessage = statusMessage;
        }
    }
}