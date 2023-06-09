using System;
using System.IO;
using System.Collections.Generic;

namespace MODB.FlatFileDB{
    public interface IKeyValDB{
        void Set(string key, string val, IEnumerable<string> tags = null);
        void Set(string key, Stream stream, IEnumerable<string> tags = null);
        void Insert(string key, string val, IEnumerable<string> tags = null);
        void Insert(string key, Stream stream, IEnumerable<string> tags = null);
        void Delete(string key);
        bool Exists(string key);
        string Get(string key);
        bool Get(string key, Action<Stream> action);
        PagedList<string> GetAll(int page = 1, int pageSize = 10);
        PagedList<string> GetByTags(IEnumerable<string> tags, int page = 1, int pageSize = 10);
        PagedList<string> GetByKeyRegexPattern(string keyRegexPattern, int page = 1, int pageSize = 10);
        PagedList<string> GetByDateRange(DateTime from, DateTime? to = null, int page = 1, int pageSize = 10);
        PagedList<string> GetTags(int page = 1, int pageSize = 10);
        PagedList<string> GetKeys(int page = 1, int pageSize = 10);
        string Name {get;}
        long Size {get;}
    }
}

