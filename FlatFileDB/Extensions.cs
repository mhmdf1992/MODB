using System;
using System.Collections.Generic;
using System.Linq;

namespace MODB.FlatFileDB{
    public static class Extensions{
        public static PagedList<T> ToPagedList<T>(this IEnumerable<T> data, int page, int pageSize, int totalItems = default){
            var total = totalItems == default ? data.Count() : totalItems;
            return new PagedList<T>(){
                Items = data,
                Page = page,
                PageSize = pageSize,
                TotalItems = total,
                TotalPages = (int)Math.Ceiling((double)total / pageSize)
            };
        }
    }
}