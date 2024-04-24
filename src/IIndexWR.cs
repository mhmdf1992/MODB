namespace MO.MODB{
    public interface IIndexWR{
        void Add(byte[] bytes);
        IndexItemToDelete DeleteByPosition(byte[] position);
        void AddRange(byte[][] range);
        void Clear();
        int CountDeleted();
        long Size {get;}
    }
}