using System.Collections.Generic;
using MO.MODB;

namespace MO.MOFile{
    public interface IKeyValueIndexBook{
        IEnumerable<ReadObject> All();
        bool Any();
        int Count();
        bool Exists(object key);
        IndexItemToDelete Delete(object key);
        IndexItemToDelete DeleteIfExists(object key);
        IndexItemToRead Find(object key);
    }
}