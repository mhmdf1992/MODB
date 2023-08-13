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
        public int Manifests {get; set;}
    }

    public class CreateDBQueryParams{
        [Required(AllowEmptyStrings = false)] public string Name {get; set;} 
        public int? Manifests {get; set;}
    }
    public class SetKeyQueryParams{
        [Required(AllowEmptyStrings = false)] public string Key {get; set;}
        public IEnumerable<string> Tags {get; set;}
        public long? TimeStamp {get; set;}
        public bool? CreateDb {get; set;}
    }
    public class GetQueryParams{
        public int Page {get; set;} = 1;
        public int PageSize {get; set;} = 10;
    }

    public class GetTagsFilteredQueryParams : GetQueryParams{
        public string Text {get; set;}
    }

    public class GetTagsOrderedQueryParams : GetTagsFilteredQueryParams{
        public bool? OrderAsc {get; set;}
        public bool? OrderDesc {get; set;}
    }

    public class GetFilteredQueryParams : GetQueryParams{
        public IEnumerable<string> Tags {get; set;}
        public long? From {get; set;}
        public long? To {get; set;}
    }

    public class GetOrderedQueryParams : GetFilteredQueryParams{
        public bool? OrderByKeyAsc {get; set;}
        public bool? OrderByKeyDesc {get; set;}
        public bool? OrderByTimeStampAsc {get; set;}
        public bool? OrderByTimeStampDesc {get; set;}
    }
}