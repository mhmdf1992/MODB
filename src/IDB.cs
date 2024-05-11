using System;
using System.Collections.Generic;
using System.IO;

namespace MO.MODB{
    public interface IDB{
        PagedList<string> All(int page = 1, int pageSize = 10);
        PagedList<string> Filter(string indexName, CompareOperators compareOperator, object value, int page = 1, int pageSize = 10);
        int Count(string indexName, CompareOperators compareOperator, object value);
        bool Any(string indexName, CompareOperators compareOperator, object value);
        string First(string indexName, CompareOperators compareOperator, object value);
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
        void Clear();
    }
}