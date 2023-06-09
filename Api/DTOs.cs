using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace MODB.Api.DTOs{
    public class DBResponse<T>{
        public T Result {get; set;}
        public string ProcessingTime {get; set;}
    }

    public class DBResponse{
        public string ProcessingTime {get; set;}
    }

    public class DBInformation{
        public string Name {get; set;}
        public long Size {get; set;}
    }

    public class CreateDBQueryParams{
        [Required(AllowEmptyStrings = false)] public string Name {get; set;} 
    }
    public class SetKeyQueryParams{
        [Required(AllowEmptyStrings = false)] public string Key {get; set;}
        public IEnumerable<string> Tags {get; set;}
    }
    public class GetPagedListQueryParams{
        public int Page {get; set;} = 1;
        public int PageSize {get; set;} = 10;
    }
}