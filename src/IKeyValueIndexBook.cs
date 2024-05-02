using System.Collections.Generic;
using MO.MODB;

namespace MO.MOFile{
    public interface IKeyValueIndexBook{
        IEnumerable<ReadObject> All();
        bool Any();
        int Count();
        bool Exists(string key);
        IndexItemToDelete Delete(string key);
        IndexItemToDelete DeleteIfExists(string key);
        IndexItemToRead Find(string key);
    }
}