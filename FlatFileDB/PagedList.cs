using System.Collections.Generic;

namespace MODB.FlatFileDB{
    public class PagedList<T>{
        public int Page {get; set;}
        public int PageSize {get; set;}
        public int TotalPages {get; set;}
        public int TotalItems {get; set;}
        public IEnumerable<T> Items {get; set;}
    }
}