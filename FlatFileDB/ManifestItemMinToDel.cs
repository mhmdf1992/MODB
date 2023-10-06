namespace MODB.FlatFileDB{
    public struct ManifestItemMinToDel{
        public ManifestItemMinToDel(long pos, int len, int man, long delPosition){
            _position = pos;
            _length = len;
            _manifest = man;
            _delPosition = delPosition;
        }
        long _delPosition;
        public long DelPosition => _delPosition;
        long _position;
        public long Position => _position;
        int _length;
        public int Length => _length;
        int _manifest;
        public int Manifest => _manifest;
    }
}