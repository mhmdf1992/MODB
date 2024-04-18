using System.Collections.Generic;

namespace MO.MODB{
    public interface IKeyValueIndexWR{
        bool Any();
        int Count();
        bool Exists(byte[] pattern);
        IndexItemToRead Find(byte[] pattern);
        IndexItemToDelete Delete(byte[] pattern);
        IEnumerable<IndexItemToRead> All();
    }
}