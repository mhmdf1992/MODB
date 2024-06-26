using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace MO.MODB{
    public class PagedList<T>{
        [JsonPropertyName("page")]
        public int Page {get; set;}
        [JsonPropertyName("pageSize")]
        public int PageSize {get; set;}
        [JsonPropertyName("items")]
        public IEnumerable<T> Items {get; set;}
    }
}