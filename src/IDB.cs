using System.Collections.Generic;
using System.IO;

namespace MO.MODB{
    public interface IDB{
        PagedList<string> All(int page = 1, int pageSize = 10);
        PagedList<string> Filter(string indexName, string pattern, CompareOperations operation, int page = 1, int pageSize = 10);
        int Count(string indexName, string pattern, CompareOperations operation);
        bool Any(string indexName, string pattern, CompareOperations operation);
        bool Any();
        int Count();
        bool Exists(string key);
        string Get(string key);
        Stream GetStream(string key);
        void Set(string key, string value, params InsertIndexItem[] index);
        void SetStream(string key, Stream value, params InsertIndexItem[] index );
        void InsertHash(Dictionary<string, string> hash, params InsertIndexHash[] index);
        void Delete(string key);
        void Clear();
    }
}