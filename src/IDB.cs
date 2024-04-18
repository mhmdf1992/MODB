using System.Collections.Generic;
using System.IO;

namespace MO.MODB{
    public interface IDB{
        PagedList<string> All(int page = 1, int pageSize = 10);
        bool Any();
        int Count();
        bool Exists(string key);
        string Get(string key);
        Stream GetStream(string key);
        void Set(string key, string value, params KeyValuePair<string,string>[] index);
        void SetStream(string key, Stream value, params KeyValuePair<string,string>[] index );
        void InsertHash(Dictionary<string, string> hash);
        void Delete(string key);
        void Clear();
    }
}