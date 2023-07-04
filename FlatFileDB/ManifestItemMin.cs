namespace MODB.FlatFileDB{
    public struct ManifestItemMin{
        public ManifestItemMin(long pos, int len){
            _position = pos;
            _length = len;
        }
        long _position;
        public long Position => _position;
        int _length;
        public int Length => _length;
    }
}