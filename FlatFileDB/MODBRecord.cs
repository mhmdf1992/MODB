using System.Collections.Generic;

namespace MODB.FlatFileDB{
    public struct MODBRecord{
        public string Key {get; set;}
        public string Value {get; set;}
        public long TimeStamp {get; set;}
        public IEnumerable<string> Tags {get; set;}
    }
}