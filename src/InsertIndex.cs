using System.Collections.Generic;

namespace MO.MODB{
    public class InsertIndexHash{
        protected string _indexName; public string IndexName => _indexName;
        protected string _indexType; public string IndexType => _indexType;
        protected Dictionary<object, object> _hash; public Dictionary<object, object> Hash => _hash;
        public InsertIndexHash(string indexName, string indexType, Dictionary<object, object> hash){
            _indexName = indexName;
            _indexType = indexType;
            _hash = hash;
        }
    }

    public class InsertIndexItem{
        protected string _indexName; public string IndexName => _indexName;
        protected object _indexValue; public object IndexValue => _indexValue;
        protected string _indexType; public string IndexType => _indexType;
        public InsertIndexItem(string name, object value, string type){
            _indexName = name;
            _indexValue = value;
            _indexType = type;
        }
    }
}