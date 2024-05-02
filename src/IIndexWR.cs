using System;
using System.Collections.Generic;
using MO.MOFile;

namespace MO.MODB{
    public interface IIndexWR{
        IEnumerable<ReadObject> Filter(Func<byte[], int, int, bool> predicate);
        int Count(Func<byte[], int, int, bool> predicate);
        bool Any(Func<byte[], int, int, bool> predicate);
        void Add(byte[] bytes);
        IndexItemToDelete DeleteByPosition(byte[] position);
        void AddRange(byte[][] range);
        void Clear();
        int CountDeleted();
        long Size {get;}
    }
}