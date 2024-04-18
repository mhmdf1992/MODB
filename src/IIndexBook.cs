using System.Collections.Generic;

namespace MO.MODB{
    public interface IIndexBook{
        string IndexName {get;}
        long Size {get;}
        int KeyMaxBytes {get;}
        void Add(string key, long valuePosition, int valueLength);
        void InsertHash(Dictionary<int,byte[][]> hash);
        byte[] GetBytes(string key, long valuePosition, int valueLength);
        void Clear();
    }
}