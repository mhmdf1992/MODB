﻿using System;
using System.IO;
using System.Collections.Generic;

namespace MODB.FlatFileDB{
    public interface IKeyValDB{
        void Set(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null);
        void Set(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null);
        void Insert(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null);
        void Insert(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null);
        void Delete(string key);
        bool Exists(string key);
        string Get(string key);
        bool Get(string key, Action<Stream> action);
        PagedList<string> Get(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, int page = 1, int pageSize = 10);
        PagedList<string> GetTags(string text = null, int page = 1, int pageSize = 10);
        PagedList<string> GetKeys(int page = 1, int pageSize = 10);
        PagedList<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, int page = 1, int pageSize = 10);
        void Clone(IKeyValDB cloneDb);
        void Rename(string name);
        void Delete();
        string Name {get;}
        long Size {get;}
        public DBConfig Config {get;}
        public DBStatus Status {get;}
    }
    public enum DBStatus{
        READY,
        CLEANING
    }
}

