using System;
using System.Collections.Generic;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public static class Extensions{
        public static PagedList<ReadObject> ToPagedList(this IEnumerable<ReadObject> data, int page, int pageSize, int? total = null){
            total ??= data.Count();
            return new PagedList<ReadObject>(){
                Items = data.Skip((page * pageSize) - pageSize).Take(pageSize),
                Page = page,
                PageSize = pageSize,
                TotalItems = total.Value,
                TotalPages = (int)Math.Ceiling((double)total / pageSize)
            };
        }

        public static PagedList<string> Read(this PagedList<ReadObject> data, IFileWR flatFileWR){
            return new PagedList<string>(){
                Items = flatFileWR.Read(data.Items),
                Page = data.Page,
                PageSize = data.PageSize,
                TotalItems = data.TotalItems,
                TotalPages = data.TotalPages
            };
        }

        public static bool CompareBytes(this byte[] haystak, byte[] needle, int offset){
            for(int i = 0; i < needle.Length; i ++){
                if(needle[i] != haystak[offset + i])
                    return false;
            }
            return true;
        }
    }
}