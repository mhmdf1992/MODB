using System.Collections.Generic;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public static class Extensions{
        public static PagedList<ReadObject> ToPagedList(this IEnumerable<ReadObject> data, int page, int pageSize){
            return new PagedList<ReadObject>(){
                Items = data.Skip((page * pageSize) - pageSize).Take(pageSize),
                Page = page,
                PageSize = pageSize
            };
        }

        public static PagedList<string> Read(this PagedList<ReadObject> data, IFileWR flatFileWR){
            return new PagedList<string>(){
                Items = flatFileWR.Read(data.Items),
                Page = data.Page,
                PageSize = data.PageSize,
            };
        }

        public static bool CompareBytes(this byte[] haystak, byte[] needle, int offset){
            for(int i = 0; i < needle.Length; i ++){
                if(needle[i] != haystak[offset + i])
                    return false;
            }
            return true;
        }

        public static bool ContainBytes(this byte[] haystak, byte[] needle, int offset, int length){
            var size = offset + length;
            for(int i = offset; i < size; i ++ ){
                if(size - i < needle.Length)
                    return false;
                if(haystak.CompareBytes(needle, i))
                    return true;
            }
            return false;
        }
    }
}