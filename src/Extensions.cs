using System;
using System.Collections.Generic;
using System.Linq;
using MO.MODB.Exceptions;
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

        public static PagedList<T> ToPagedList<T>(this IEnumerable<T> data, int page, int pageSize){
            return new PagedList<T>(){
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

        public static bool CompareBytes(this byte[] haystak, byte[] needle, int offset, int length){
            if(length != needle.Length)
                return false;
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

        public static byte[] ToBytes(this object obj, string dataType){
            try{
                return Converter.ToBytes[dataType](obj);
            }catch{
                throw new KeyIndexTypeMissMatchException(obj, obj.GetType().Name, dataType);
            }
        } 
        public static dynamic To(this byte[] bytes, string dataType, int offset) => Converter.To[dataType](bytes, offset);
        public static bool IsSupportedType(this string dataType, string indexName) => Validator.ValidateType(dataType, indexName);
        public static bool IsSupportedKeyType(this string dataType, string indexName) => Validator.ValidateKeyType(dataType, indexName);
        public static bool IsValidIndexValue(this byte[] bytesValue, string indexName, object value, string indexType ) => Validator.ValidateIndexValue(bytesValue, indexName, value, indexType);
        public static bool IsValidIndexName(this string name) => Validator.ValidateIndexName(name);
        public static bool IsValidDBName(this string name) => Validator.ValidateDBName(name);
        public static bool IsValid(this CompareOperators compareOperator, string dataType) => Validator.ValidateCompareOperatorWithDataType(compareOperator, dataType);
        public static Func<byte[], int, int, bool> ToPredicate(this CompareOperators compareOperator, byte[] patternBytes, string type) => COMPARE_OPERATOR_PREDICATE[compareOperator](patternBytes.To(type, 0), patternBytes, type);
        public static Dictionary<CompareOperators,Func<dynamic, byte[], string, Func<byte[], int, int, bool>>> COMPARE_OPERATOR_PREDICATE = new Dictionary<CompareOperators, Func<dynamic, byte[], string, Func<byte[], int, int, bool>>>(){
            { CompareOperators.Equal, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.CompareBytes(patternBytes, offset, length)},
            { CompareOperators.NotEqual, (pattern, patternBytes, type) => (haystack, offset, length) => !haystack.CompareBytes(patternBytes, offset, length)},
            { CompareOperators.Contain, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.ContainBytes(patternBytes, offset, length)},
            { CompareOperators.NotContain, (pattern, patternBytes, type) => (haystack, offset, length) => !haystack.ContainBytes(patternBytes, offset, length)},
            { CompareOperators.GreaterThan, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.To(type, offset) > pattern},
            { CompareOperators.GreaterThanOrEqual, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.To(type, offset) >= pattern},
            { CompareOperators.LessThan, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.To(type, offset) < pattern},
            { CompareOperators.LessThanOrEqual, (pattern, patternBytes, type) => (haystack, offset, length) => haystack.To(type, offset) <= pattern}
        };
    }
}