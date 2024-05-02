using System.Collections.Generic;
using MO.MOFile;

namespace MO.MODB{
    public interface IIndexBook{
        string IndexName {get;}
        long Size {get;}
        int KeyMaxBytes {get;}
        PagedList<ReadObject> Filter(string pattern, CompareOperations operation, int page = 1, int pageSize = 10);
        int Count(string pattern, CompareOperations operation);
        bool Any(string pattern, CompareOperations operation);
        IndexItemToDelete DeleteByPosition(long position);
        void Add(string key, long valuePosition, int valueLength);
        void InsertHash(Dictionary<int,byte[][]> hash);
        byte[] GetBytes(string key, long valuePosition, int valueLength);
        void Clear();
    }
}