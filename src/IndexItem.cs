using System.Collections.Generic;
using MO.MOFile;

namespace MO.MODB{
    public class IndexItem{
        long _valuePosition; public long ValuePosition => _valuePosition;
        int _valuelength; public int ValueLength => _valuelength;
        public IndexItem(long valuePosition, int valueLength){
            _valuePosition = valuePosition;
            _valuelength = valueLength;
        }
    }
    public class IndexItemToWrite : IndexItem{
        object _key; public object Key => _key;
        public IndexItemToWrite(object key, long valuePosition, int valueLength):base(valuePosition, valueLength){
            _key = Key;
        }
    }
    public class IndexItemToRead : IndexItem{
        string _index; public string Index => _index;
        long _indexPosition; public long IndexPosition => _indexPosition;
        public IndexItemToRead(string index, long indexPosition, long valuePosition, int valueLength):base(valuePosition, valueLength){
            _index = index;
            _indexPosition = indexPosition;
        }
    }

    public class IndexItemToDelete : IndexItemToRead{
        bool _deleted; public bool Deleted => _deleted;
        public IndexItemToDelete(bool deleted, string index, long indexPosition, long valuePosition, int valueLength):base(index, indexPosition, valuePosition, valueLength){
            _deleted = deleted;
        }
    }

    public class InsertIndexHash{
        protected string _indexName; public string IndexName => _indexName;
        protected Dictionary<string,string> _hash; public Dictionary<string,string> Hash => _hash;
        public InsertIndexHash(string indexName, Dictionary<string,string> hash){
            _indexName = indexName;
            _hash = hash;
        }
    }

    public class InsertIndexItem{
        protected string _indexName; public string IndexName => _indexName;
        protected string _value; public string Value => _value;
        public InsertIndexItem(string indexName, string value){
            _indexName = indexName;
            _value = value;
        }
    }
}