using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MO.MOFile;

namespace MO.MODB{
    public class DB : IDB
    {
        protected string _path;
        protected IDataWR _dataWR;
        protected Dictionary<string, IIndexBook> _indexBooks;
        protected string _name;
        public string Name => _name;
        public long Size => _dataWR.Size + _indexBooks.Values.Sum(x => x.Size);
        public DB(string path){
            _path = path;
            _name = Path.GetFileName(_path);
            _dataWR = new DataWR(Path.Combine(path, $"{_name}.dat"));
            _indexBooks = new Dictionary<string, IIndexBook>();
            LoadIndexes();
        }

        protected void LoadIndexes(){
            var dirs = Directory.GetDirectories(_path, "*.index");
            if(dirs == null || !dirs.Any()){
                _indexBooks.Add("key", new KeyValueIndexBook("key", Path.Combine(_path, "key.index")));
                return;
            }   
            foreach(var dir in dirs){
                var name = Path.GetFileNameWithoutExtension(dir);
                if(name == "key"){
                    _indexBooks.Add(name, new KeyValueIndexBook(name, dir));
                    continue;
                }
                _indexBooks.Add(name, new KeyIndexBook(name, dir));
            }
        }

        protected void Insert(string key, string value, params InsertIndexItem[] index){
            var keyIndexBook = _indexBooks["key"];
            var position = _dataWR.Add(value);
            if(position == -1)
                return;
            var length = _dataWR.Encoding.GetByteCount(value);
            keyIndexBook.Add(key, position, length);
            if(index == null && !index.Any())
                return;
            foreach(var i in index){
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName))
                    _indexBooks.Add(indexName, new KeyIndexBook(indexName, Path.Combine(_path, $"{indexName}.index")));
                _indexBooks[indexName].Add(i.Value, position, length);
            }
        }

        protected void Insert(string key, Stream stream, params InsertIndexItem[] index){
            var keyIndexBook = _indexBooks["key"];
            var length = stream.Length;
            var position = _dataWR.Add(stream);
            if(position == -1)
                return;
            keyIndexBook.Add(key, position, (int)length);
            if(index == null && !index.Any())
                return;
            foreach(var i in index){
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName))
                    _indexBooks.Add(indexName, new KeyIndexBook(indexName, Path.Combine(_path, $"{indexName}.index")));
                _indexBooks[indexName].Add(i.Value, position, (int)length);
            }
        }

        public bool Any() => ((IKeyValueIndexBook)_indexBooks["key"]).Any();

        public int Count() => ((IKeyValueIndexBook)_indexBooks["key"]).Count();

        public string Get(string key){
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).Find(key) ?? throw new Exceptions.KeyNotFoundException(key);
            return _dataWR.Get(indexItem.ValuePosition, indexItem.ValueLength);
        }

        public Stream GetStream(string key){
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).Find(key) ?? throw new Exceptions.KeyNotFoundException(key);
            return _dataWR.GetStream(indexItem.ValuePosition, indexItem.ValueLength);
        }

        public bool Exists(string key){
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            return ((IKeyValueIndexBook)keyBook).Exists(key);
        }

        public void Set(string key, string value, params InsertIndexItem[] index)
        {
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).DeleteIfExists(key);
            if(indexItem != null && indexItem.Deleted){
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
                var otherIndexBooks = _indexBooks.Where(x => x.Key != "key");
                if(otherIndexBooks != null && otherIndexBooks.Any()){
                    foreach(var indexBook in otherIndexBooks){
                        indexBook.Value.DeleteByPosition(indexItem.ValuePosition);
                    }
                }
            }
            Insert(key, value, index);
        }

        public void SetStream(string key, Stream stream, params InsertIndexItem[] index)
        {
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).DeleteIfExists(key);
            if(indexItem != null && indexItem.Deleted){
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
                var otherIndexBooks = _indexBooks.Where(x => x.Key != "key");
                if(otherIndexBooks != null && otherIndexBooks.Any()){
                    foreach(var indexBook in otherIndexBooks){
                        indexBook.Value.DeleteByPosition(indexItem.ValuePosition);
                    }
                }
            }
            Insert(key, stream, index);
        }

        public void Delete(string key){
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).Delete(key);
            if(indexItem.Deleted){
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
                var otherIndexBooks = _indexBooks.Where(x => x.Key != "key");
                if(otherIndexBooks != null && otherIndexBooks.Any()){
                    foreach(var indexBook in otherIndexBooks){
                        indexBook.Value.DeleteByPosition(indexItem.ValuePosition);
                    }
                }
            }
        }

        public void InsertHash(Dictionary<string, string> hash, params InsertIndexHash[] index){
            var keyBook = _indexBooks["key"];
            foreach(var pair in hash){
                Validator.ValidateKey(pair.Key, keyBook.KeyMaxBytes);
            }
            var keyPositionList = _dataWR.FlatFileWR.AppendList(hash.ToArray()).ToArray();
            var keyIndexHash = keyPositionList.Select(pair => new {keyBytesLength = Encoding.UTF8.GetByteCount(pair.Key), indexItemBytes = keyBook.GetBytes(pair.Key, pair.Value.Position, pair.Value.Length)})
                .GroupBy(obj => obj.keyBytesLength,
                        obj => obj.indexItemBytes,
                        (key, grp) => new KeyValuePair<int,byte[][]>(key, grp.ToArray())).ToDictionary(x => x.Key, x => x.Value);
            keyBook.InsertHash(keyIndexHash);
            if(index == null && !index.Any())
                return;
            foreach(var i in index){
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName))
                    _indexBooks.Add(indexName, new KeyIndexBook(indexName, Path.Combine(_path, $"{indexName}.index")));
                var indexBook = _indexBooks[indexName];
                var indexHash = keyPositionList.Join(i.Hash, keyPosList => keyPosList.Key, hash => hash.Key, (keyPosList, hash) => new {key = hash.Value, val = keyPosList.Value})
                    .Select(indexItem => new {keyBytesLength = Encoding.UTF8.GetByteCount(indexItem.key), indexItemBytes = indexBook.GetBytes(indexItem.key, indexItem.val.Position, indexItem.val.Length)})
                    .GroupBy(obj => obj.keyBytesLength,
                            obj => obj.indexItemBytes,
                            (key, grp) => new KeyValuePair<int,byte[][]>(key, grp.ToArray())).ToDictionary(x => x.Key, x => x.Value);
                indexBook.InsertHash(indexHash);
            }
        }

        public PagedList<string> All(int page = 1, int pageSize = 10){
            var keyBook = (IKeyValueIndexBook)_indexBooks["key"];
            return keyBook.All().ToPagedList(page, pageSize).Read(_dataWR.FlatFileWR);
        }

        public PagedList<string> Filter(string indexName, string pattern, CompareOperations operation, int page = 1, int pageSize = 10){
            if(!_indexBooks.ContainsKey(indexName.ToLower()))
                throw new ArgumentException($"Index {indexName} does not exist", paramName: nameof(indexName));
            return _indexBooks[indexName.ToLower()].Filter(pattern, operation, page, pageSize).Read(_dataWR.FlatFileWR);
        }

        public int Count(string indexName, string pattern, CompareOperations operation){
            if(!_indexBooks.ContainsKey(indexName.ToLower()))
                throw new ArgumentException($"Index {indexName} does not exist", paramName: nameof(indexName));
            return _indexBooks[indexName.ToLower()].Count(pattern, operation);
        }

        public bool Any(string indexName, string pattern, CompareOperations operation){
            if(!_indexBooks.ContainsKey(indexName.ToLower()))
                throw new ArgumentException($"Index {indexName} does not exist", paramName: nameof(indexName));
            return _indexBooks[indexName.ToLower()].Any(pattern, operation);
        }

        public void Clear()
        {
            foreach(var book in _indexBooks.Values){
                book.Clear();
            }
        }
    }
}