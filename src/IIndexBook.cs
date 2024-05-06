using System;
using System.Collections.Generic;
using MO.MOFile;

namespace MO.MODB{
    public interface IIndexBook{
        string IndexName {get;}
        string IndexType {get;}
        long Size {get;}
        int KeyMaxBytes {get;}
        bool IsKeyIndex {get;}
        IndexItemToRead FindFirst(object key);
        PagedList<ReadObject> Filter(object pattern, CompareOperators compareOperator, int page = 1, int pageSize = 10);
        int Count(object pattern, CompareOperators compareOperator);
        bool Any(object pattern, CompareOperators compareOperator);
        IndexItemToDelete DeleteByPosition(long position);
        void Add(object key, long valuePosition, int valueLength);
        void InsertHash(Dictionary<int,byte[][]> hash);
        byte[] GetBytes(object key, long valuePosition, int valueLength);
        void Clear();
    }
}