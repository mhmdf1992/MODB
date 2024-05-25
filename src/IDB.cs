using System;
using System.Collections.Generic;
using System.IO;

namespace MO.MODB{
    public interface IDB{
        string Name {get;}
        long Size {get;}
        IEnumerable<Index> Indexes {get;}
        PagedList<string> All(int page = 1, int pageSize = 10);
        PagedList<string> Filter(string indexName = null, CompareOperators? compareOperator = null, object value = null, int page = 1, int pageSize = 10);
        int Count(string indexName = null, CompareOperators? compareOperator = null, object value = null);
        bool Any(string indexName = null, CompareOperators? compareOperator = null, object value = null);
        bool Any();
        int Count();
        bool Exists(object key);
        string Get(object key);
        Stream GetStream(object key);
        void Set(string key, string value);
        void SetStream(string key, Stream value);
        void Set(object key, string value, string keyType, params InsertIndexItem[] index);
        void SetStream(object key, Stream value, string keyType, params InsertIndexItem[] index );
        void InsertHash(Dictionary<object, string> hash, string keyType, params InsertIndexHash[] index);
        void Delete(object key);
        void Delete();
        void Clear();
    }
}