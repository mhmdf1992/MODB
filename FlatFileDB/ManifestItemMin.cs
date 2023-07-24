

namespace MODB.FlatFileDB{
    public struct ManifestItemMin{
        public ManifestItemMin(long pos, int len, int man){
            _position = pos;
            _length = len;
            _manifest = man;
        }
        long _position;
        public long Position => _position;
        int _length;
        public int Length => _length;
        int _manifest;
        public int Manifest => _manifest;
    }
}