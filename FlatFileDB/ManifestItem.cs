using System;
using System.IO;

namespace MODB.FlatFileDB{
    public struct ManifestItem{
        long _timeStamp;
        public long timeStamp => _timeStamp;
        string _tags;
        public string Tags => _tags;
        string _key;
        long _position;
        int _length;
        public string Key => _key;
        public long Position => _position;
        public int Length => _length;
        
        public ManifestItem(string key, long pos, int len, long timeStamp, string tags = null){
            _key = key;
            _position = pos;
            _length = len;
            _timeStamp = timeStamp;
            _tags = tags;
        }
        public string ToCsv() => $"{_key},{_position},{_length},{_timeStamp},{(string.IsNullOrEmpty(_tags) ? "" : _tags)}{Environment.NewLine}";
        public Stream ToCsvStream() => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ToCsv()));
    }
}