using System;
using System.IO;
namespace MODB.FlatFileDB{
    public struct ManifestItem{
        int _manifest;
        public int Manifest => _manifest;
        long _timeStamp;
        public long TimeStamp => _timeStamp;
        string _tags;
        public string Tags => _tags;
        string _key;
        long _position;
        int _length;
        public string Key => _key;
        public long Position => _position;
        public int Length => _length;
        uint _isDeleted;
        
        public ManifestItem(string key, long pos, int len, long timeStamp, int manifest, string tags = null){
            _key = key;
            _position = pos;
            _length = len;
            _timeStamp = timeStamp;
            _tags = tags;
            _manifest = manifest;
            _isDeleted = 0;
        }
        public string ToCsv() => $"{_key},{_position},{_length},{_timeStamp},{(string.IsNullOrEmpty(_tags) ? "" : _tags)},{_isDeleted}{Environment.NewLine}";
        public Stream ToCsvStream() => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ToCsv()));
    }
}