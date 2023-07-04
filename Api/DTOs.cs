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
        public long? TimeStamp {get; set;}
    }
    public class GetPagedListQueryParams{
        public int Page {get; set;} = 1;
        public int PageSize {get; set;} = 10;
    }

    public class GetTagsPagedListQueryParams : GetPagedListQueryParams{
        public bool? OrderAsc {get; set;}
        public bool? OrderDesc {get; set;}
    }

    public class GetFilteredPagedListQueryParams : GetPagedListQueryParams{
        public IEnumerable<string> Tags {get; set;}
        public long? From {get; set;}
        public long? To {get; set;}
        public bool? OrderByKeyAsc {get; set;}
        public bool? OrderByKeyDesc {get; set;}
        public bool? OrderByTimeStampAsc {get; set;}
        public bool? OrderByTimeStampDesc {get; set;}
    }
}