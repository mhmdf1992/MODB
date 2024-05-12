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
}