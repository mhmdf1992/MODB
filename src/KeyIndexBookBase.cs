using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MO.MOFile;

namespace MO.MODB{
    public abstract class KeyIndexBookBase{
        protected Encoding _encoding;
        protected string _path;
        protected string _indexName; public string IndexName => _indexName;
        protected bool IsKeyIndex => _indexName == "key";
        protected const int POSITION_BYTES = 8;
        protected const int LENGTH_BYTES = 4;
        protected const int KEY_MAX_BYTES = 64;
        public int KeyMaxBytes => KEY_MAX_BYTES;
        public long Size => _indexWRs.Values.Sum(x => x.Size);
        protected Dictionary<int, IIndexWR> _indexWRs;
        public KeyIndexBookBase(string indexName, string path){
            _indexName = indexName;
            _path = path;
            _encoding = Encoding.UTF8;
            _indexWRs = new Dictionary<int, IIndexWR>();
            if(!Directory.Exists(_path)){
                Directory.CreateDirectory(_path);
            }
            LoadIndexWRs();
        }

        protected void LoadIndexWRs(){
            var files = Directory.GetFiles(_path, "*.index");
            if(files == null || !files.Any())
                return;
            foreach(var file in files){
                var name = Path.GetFileNameWithoutExtension(file);
                var keyBytes = int.Parse(name);
                if(IsKeyIndex){
                    _indexWRs.Add(keyBytes, new KeyValueIndexWR(
                        indexName: _indexName,
                        numberOfKeyBytes: keyBytes,
                        numberOfPositionBytes: POSITION_BYTES,
                        numberOfLengthBytes: LENGTH_BYTES,
                        path: file
                        ));
                    continue;
                }
                _indexWRs.Add(keyBytes, new KeyIndexWR(
                    indexName: _indexName,
                    numberOfKeyBytes: keyBytes,
                    numberOfPositionBytes: POSITION_BYTES,
                    numberOfLengthBytes: LENGTH_BYTES,
                    path: file
                    ));
            }
        }

        public byte[] GetBytes(string key, long valuePosition, int valueLength){
            var keyBytes = _encoding.GetBytes(key);
            if(keyBytes.Length > KEY_MAX_BYTES)
                throw new ArgumentException(message: $"{key} is not a valid key. keys must be {KEY_MAX_BYTES} characters maximum length.", paramName: nameof(key));
            byte[] bytesToWrite = new byte[keyBytes.Length + POSITION_BYTES + LENGTH_BYTES];
            Buffer.BlockCopy(keyBytes, 0, bytesToWrite, 0, keyBytes.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(valuePosition), 0, bytesToWrite, keyBytes.Length, POSITION_BYTES);
            Buffer.BlockCopy(BitConverter.GetBytes(valueLength), 0, bytesToWrite, keyBytes.Length + POSITION_BYTES, LENGTH_BYTES);
            return bytesToWrite;
        }

        public void Add(string key, long valuePosition, int valueLength)
        {
            var keyBytesLength = _encoding.GetByteCount(key);
            var bytesToWrite = GetBytes(key, valuePosition, valueLength);
            if(!_indexWRs.ContainsKey(keyBytesLength)){
                _indexWRs.Add(keyBytesLength, 
                    IsKeyIndex ? 
                    new KeyValueIndexWR(
                        indexName: _indexName,
                        numberOfKeyBytes: keyBytesLength,
                        numberOfPositionBytes: POSITION_BYTES,
                        numberOfLengthBytes: LENGTH_BYTES,
                        path: Path.Combine(_path, $"{keyBytesLength}.index")) :
                    new KeyIndexWR(
                        indexName: _indexName,
                        numberOfKeyBytes: keyBytesLength,
                        numberOfPositionBytes: POSITION_BYTES,
                        numberOfLengthBytes: LENGTH_BYTES,
                        path: Path.Combine(_path, $"{keyBytesLength}.index")
                        ) as IIndexWR);
            }
            _indexWRs[keyBytesLength].Add(bytesToWrite);
        }

        public void InsertHash(Dictionary<int,byte[][]> hash){
            foreach(var pair in hash){
                if(!_indexWRs.ContainsKey(pair.Key)){
                    _indexWRs.Add(pair.Key, 
                        IsKeyIndex ? new KeyValueIndexWR(
                            indexName: _indexName,
                            numberOfKeyBytes: pair.Key,
                            numberOfPositionBytes: POSITION_BYTES,
                            numberOfLengthBytes: LENGTH_BYTES,
                            path: Path.Combine(_path, $"{pair.Key}.index")) :
                            new KeyIndexWR(
                                indexName: _indexName,
                                numberOfKeyBytes: pair.Key,
                                numberOfPositionBytes: POSITION_BYTES,
                                numberOfLengthBytes: LENGTH_BYTES,
                                path: Path.Combine(_path, $"{pair.Key}.index")
                                ) as IIndexWR);
                }
                _indexWRs[pair.Key].AddRange(pair.Value);
            }
        }

        public void Clear(){
            if(!_indexWRs.Any())
                return;
            foreach(var wr in _indexWRs.Values){
                wr.Clear();
            }
        }

        public PagedList<ReadObject> Filter(string pattern, CompareOperations operation, int page = 1, int pageSize = 10){
            if(!_indexWRs.Any())
                return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
            var patternBytes = _encoding.GetBytes(pattern);
            if(operation.Equals(CompareOperations.Equal)){
                if(!_indexWRs.ContainsKey(patternBytes.Length))
                    return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
                var res =  _indexWRs[patternBytes.Length].Filter((haystack, offset, length) => haystack.CompareBytes(patternBytes, offset));
                return res.ToPagedList(page, pageSize);
            }
            var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
            if(targetWR == null || !targetWR.Any())
                return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
            var result = targetWR.Select(x => x.Value.Filter((haystack, offset, length) => haystack.ContainBytes(patternBytes, offset, length)));
            return result.SelectMany(x => x).ToPagedList(page, pageSize);
        }

        public int Count(string pattern, CompareOperations operation){
            if(!_indexWRs.Any())
                return 0;
            var patternBytes = _encoding.GetBytes(pattern);
            if(operation.Equals(CompareOperations.Equal)){
                if(!_indexWRs.ContainsKey(patternBytes.Length))
                    return 0;
                return _indexWRs[patternBytes.Length].Count((haystack, offset, length) => haystack.CompareBytes(patternBytes, offset));
            }
            var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
            if(targetWR == null || !targetWR.Any())
                return 0;
            return targetWR.Select(x => x.Value.Count((haystack, offset, length) => haystack.ContainBytes(patternBytes, offset, length))).Sum(x => x);
        }

        public bool Any(string pattern, CompareOperations operation){
            if(!_indexWRs.Any())
                return false;
            var patternBytes = _encoding.GetBytes(pattern);
            if(operation.Equals(CompareOperations.Equal)){
                if(!_indexWRs.ContainsKey(patternBytes.Length))
                    return false;
                return _indexWRs[patternBytes.Length].Any((haystack, offset, length) => haystack.CompareBytes(patternBytes, offset));
            }
            var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
            if(targetWR == null || !targetWR.Any())
                return false;
            return targetWR.Select(x => x.Value.Any((haystack, offset, length) => haystack.ContainBytes(patternBytes, offset, length))).Any(x => x);
        }

        public IndexItemToDelete DeleteByPosition(long position)
        {
            if(!_indexWRs.Any())
                return default;
            var pattern = BitConverter.GetBytes(position);
            foreach(var indexWR in _indexWRs.Values){
                var res = indexWR.DeleteByPosition(pattern);
                if(res == null)
                    continue;
                return res;
            }
            return default;
        }
    }
}