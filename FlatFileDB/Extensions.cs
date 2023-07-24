using System;
using System.Collections.Generic;
using System.Linq;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public static class Extensions{
        public static PagedList<T> ToPagedList<T>(this IEnumerable<T> data, int page, int pageSize){
            var total = data.Count();
            return new PagedList<T>(){
                Items = data.Skip((page * pageSize) - pageSize).Take(pageSize),
                Page = page,
                PageSize = pageSize,
                TotalItems = total,
                TotalPages = (int)Math.Ceiling((double)total / pageSize)
            };
        }

        public static IEnumerable<string> Read(this IEnumerable<ManifestItem> data, IFileWR flatFileWR){
            return flatFileWR.Read(data.Select(x => new ReadObj(x.Position, x.Length)));
        }

        public static PagedList<string> Read(this PagedList<ManifestItemMin> data, IFileWR flatFileWR){
            return new PagedList<string>(){
                Items = flatFileWR.Read(data.Items.Select(x => new ReadObj(x.Position, x.Length))),
                Page = data.Page,
                PageSize = data.PageSize,
                TotalItems = data.TotalItems,
                TotalPages = data.TotalPages
            };
        }

        public static PagedList<string> Read(this PagedList<ManifestItem> data, IFileWR flatFileWR){
            return new PagedList<string>(){
                Items = flatFileWR.Read(data.Items.Select(x => new ReadObj(x.Position, x.Length))),
                Page = data.Page,
                PageSize = data.PageSize,
                TotalItems = data.TotalItems,
                TotalPages = data.TotalPages
            };
        }

        public static PagedList<string> Keys(this PagedList<ManifestItem> data){
            return new PagedList<string>(){
                Items = data.Items.Select(x => x.Key),
                Page = data.Page,
                PageSize = data.PageSize,
                TotalItems = data.TotalItems,
                TotalPages = data.TotalPages
            };
        }

        public static PagedList<string> ToOrderedPagedList(this IEnumerable<string> data, int page, int pageSize, bool? orderAsc, bool? orderDesc){
            if(orderAsc == true)
                return data
                    .OrderBy(x => x)
                    .ToPagedList(page, pageSize);
            if(orderDesc == true)
                return data
                    .OrderByDescending(x => x)
                    .ToPagedList(page, pageSize);
            return data
                .ToPagedList(page, pageSize);
        }

        public static PagedList<ManifestItem> ToOrderedPagedList(this IEnumerable<ManifestItem> data, int page, int pageSize, bool? orderByKeyAsc = null, bool? orderByKeyDesc = null, bool? orderByTimeStampAsc = null, bool? orderByTimeStampDesc = null){
            if(orderByKeyAsc == true)
                return data
                    .OrderBy(x => x.Key)
                    .ToPagedList(page, pageSize);
            if(orderByKeyDesc == true)
                return data
                    .OrderByDescending(x => x.Key)
                    .ToPagedList(page, pageSize);
            if(orderByTimeStampAsc == true)
                return data
                    .OrderBy(x => x.TimeStamp)
                    .ToPagedList(page, pageSize);
            if(orderByTimeStampDesc == true)
                return data
                    .OrderByDescending(x => x.TimeStamp)
                    .ToPagedList(page, pageSize);
            return data
                .ToPagedList(page, pageSize);
        }
    }
}