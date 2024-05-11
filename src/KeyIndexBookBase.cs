using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MO.MOFile;

namespace MO.MODB{
    public abstract class KeyIndexBookBase{
        protected string _path;
        protected string _indexName; public string IndexName => _indexName;
        protected string _indexType; public string IndexType => _indexType;
        public bool IsKeyIndex => _indexName == "key";
        protected const int POSITION_BYTES = 8;
        protected const int LENGTH_BYTES = 4;
        protected const int KEY_MAX_BYTES = 64;
        public int KeyMaxBytes => KEY_MAX_BYTES;
        public long Size => _indexWRs.Values.Sum(x => x.Size);
        protected ConcurrentDictionary<int, IIndexWR> _indexWRs;
        public KeyIndexBookBase(string indexName, string indexType, string path){
            _indexName = indexName;
            _indexType = indexType;
            _path = path;
            _indexWRs = new ConcurrentDictionary<int, IIndexWR>();
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
                    _indexWRs.TryAdd(keyBytes, new KeyValueIndexWR(
                        indexName: _indexName,
                        numberOfKeyBytes: keyBytes,
                        numberOfPositionBytes: POSITION_BYTES,
                        numberOfLengthBytes: LENGTH_BYTES,
                        path: file
                        ));
                    continue;
                }
                _indexWRs.TryAdd(keyBytes, new KeyIndexWR(
                    indexName: _indexName,
                    numberOfKeyBytes: keyBytes,
                    numberOfPositionBytes: POSITION_BYTES,
                    numberOfLengthBytes: LENGTH_BYTES,
                    path: file
                    ));
            }
        }

        public byte[] GetBytes(object key, long valuePosition, int valueLength){
            var keyBytes = key.ToBytes(_indexType);
            byte[] bytesToWrite = new byte[keyBytes.Length + POSITION_BYTES + LENGTH_BYTES];
            Buffer.BlockCopy(keyBytes, 0, bytesToWrite, 0, keyBytes.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(valuePosition), 0, bytesToWrite, keyBytes.Length, POSITION_BYTES);
            Buffer.BlockCopy(BitConverter.GetBytes(valueLength), 0, bytesToWrite, keyBytes.Length + POSITION_BYTES, LENGTH_BYTES);
            return bytesToWrite;
        }

        public void Add(object key, long valuePosition, int valueLength)
        {
            var keyBytesLength = key.ToBytes(_indexType).Length;
            var bytesToWrite = GetBytes(key, valuePosition, valueLength);
            if(!_indexWRs.ContainsKey(keyBytesLength)){
                _indexWRs.TryAdd(keyBytesLength, 
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
                    _indexWRs.TryAdd(pair.Key, 
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

        public PagedList<ReadObject> Filter(object pattern, CompareOperators compareOperator, int page = 1, int pageSize = 10){
            if(!_indexWRs.Any())
                return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
            var patternBytes = pattern.ToBytes(_indexType);
            var predicate = compareOperator.ToPredicate(patternBytes, _indexType);
            if(compareOperator.Equals(CompareOperators.Contain)){
                var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
                if(targetWR == null || !targetWR.Any())
                    return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
                return targetWR.Select(x => x.Value.Filter(predicate)).SelectMany(x => x).ToPagedList(page, pageSize);
            }
            if(!_indexWRs.ContainsKey(patternBytes.Length))
                return Enumerable.Empty<ReadObject>().ToPagedList(page, pageSize);
            return _indexWRs[patternBytes.Length].Filter(predicate).ToPagedList(page, pageSize);
        }

        public int Count(object pattern, CompareOperators compareOperator){
            if(!_indexWRs.Any())
                return 0;
            var patternBytes = pattern.ToBytes(_indexType);
            var predicate = compareOperator.ToPredicate(patternBytes, _indexType);
            if(compareOperator.Equals(CompareOperators.Contain)){
                var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
                if(targetWR == null || !targetWR.Any())
                    return 0;
                return targetWR.Select(x => x.Value.Count(predicate)).Sum(x => x);
            }
            if(!_indexWRs.ContainsKey(patternBytes.Length))
                return 0;
            return _indexWRs[patternBytes.Length].Count(predicate);
        }

        public bool Any(object pattern, CompareOperators compareOperator){
            if(!_indexWRs.Any())
                return false;
            var patternBytes = pattern.ToBytes(_indexType);
            var predicate = compareOperator.ToPredicate(patternBytes, _indexType);
            if(compareOperator.Equals(CompareOperators.Contain)){
                var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
                if(targetWR == null || !targetWR.Any())
                    return false;
                foreach(var wr in _indexWRs){
                    if(wr.Value.Any(predicate))
                        return true;
                }
                return false;
            }
            if(!_indexWRs.ContainsKey(patternBytes.Length))
                return false;
            return _indexWRs[patternBytes.Length].Any(predicate);
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

        public IndexItemToRead FindFirst(object pattern, CompareOperators compareOperator)
        {
            if(!_indexWRs.Any())
                return default;
            var patternBytes = pattern.ToBytes(_indexType);
            var predicate = compareOperator.ToPredicate(patternBytes, _indexType);
            if(compareOperator.Equals(CompareOperators.Contain)){
                var targetWR = _indexWRs.Where(pair => pair.Key >= patternBytes.Length);
                if(targetWR == null || !targetWR.Any())
                    return default;
                foreach(var wr in _indexWRs){
                    var indexItem = wr.Value.FindFirst(predicate);
                    if(indexItem == default)
                        continue;
                    return indexItem;
                }
                return default;
            }
            if(!_indexWRs.ContainsKey(patternBytes.Length))
                return default;
            return _indexWRs[patternBytes.Length].FindFirst(predicate);
        }
    }
}