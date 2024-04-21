using System.Collections.Generic;

namespace MO.MODB{
    public interface IKeyValueIndexWR{
        bool Any();
        int Count();
        bool Exists(byte[] key);
        IndexItemToRead Find(byte[] key);
        IndexItemToDelete Delete(byte[] key);
        IEnumerable<IndexItemToRead> All();
    }
}