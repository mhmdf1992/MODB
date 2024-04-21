using System;
using System.Collections.Generic;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public static class Extensions{
        public static PagedList<IndexItem> ToPagedList(this IEnumerable<IndexItem> data, int page, int pageSize){
            var total = data.Count();
            return new PagedList<IndexItem>(){
                Items = data.Skip((page * pageSize) - pageSize).Take(pageSize),
                Page = page,
                PageSize = pageSize,
                TotalItems = total,
                TotalPages = (int)Math.Ceiling((double)total / pageSize)
            };
        }

        public static PagedList<string> Read(this PagedList<IndexItem> data, IFileWR flatFileWR){
            return new PagedList<string>(){
                Items = flatFileWR.Read(data.Items.Select(x => new ReadObject(x.ValuePosition, x.ValueLength))),
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