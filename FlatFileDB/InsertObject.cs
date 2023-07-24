using System.Collections.Generic;

namespace MODB.FlatFileDB{
    public struct InsertObject{
        public string Key {get; set;}
        public string Value {get; set;}
        public IEnumerable<string> Tags {get; set;}
        public long? TimeStamp {get; set;}
    }
}