namespace MODB.ConcurrentFile{
    public struct ReadObj{
        public ReadObj(long startPosition, int? length){
            _position = startPosition;
            _length = length;
        }
        long _position; public long Position => _position;
        int? _length; public int? Length => _length;
    }
}