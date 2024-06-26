using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MO.MODB.Exceptions;
using MO.MOFile;

namespace MO.MODB{
    public class DB : IDB
    {
        protected string _path;
        protected IDataWR _dataWR;
        protected ConcurrentDictionary<string, IIndexBook> _indexBooks;
        protected string _name; public string Name => Path.GetFileNameWithoutExtension(_name);
        public long Size => _dataWR.Size + _indexBooks.Values.Sum(x => x.Size);
        public IEnumerable<Index> Indexes => _indexBooks.Values.Select(x => new Index(){ Name = x.IndexName, Type = x.IndexType});
        public DB(string path){
            _path = path;
            _name = Path.GetFileName(_path);
            _dataWR = new DataWR(Path.Combine(path, $"{_name}.dat"));
            _indexBooks = new ConcurrentDictionary<string, IIndexBook>();
            LoadIndexes();
        }

        protected void LoadIndexes(){
            var dirs = Directory.GetDirectories(_path, "*.index");
            if(dirs == null || !dirs.Any())
                return;
            foreach(var dir in dirs){
                var nameAndType = Path.GetFileNameWithoutExtension(dir);
                var name = nameAndType.Split('.')[0];
                var type = nameAndType.Split('.')[1];
                if(name == "key"){
                    _indexBooks.TryAdd(name, new KeyValueIndexBook(name, type, dir));
                    continue;
                }
                _indexBooks.TryAdd(name, new KeyIndexBook(name, type, dir));
            }
        }

        protected void Insert(object key, string value, params InsertIndexItem[] index){
            var keyIndexBook = _indexBooks["key"];
            var position = _dataWR.Add(value);
            if(position == -1)
                return;
            var length = _dataWR.Encoding.GetByteCount(value);
            keyIndexBook.Add(key, position, length);
            if(index == null || !index.Any())
                return;
            foreach(var i in index){
                i.IndexName.IsValidIndexName();
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName)){
                    i.IndexType.IsSupportedType(i.IndexName);
                    _indexBooks.TryAdd(indexName, new KeyIndexBook(indexName, i.IndexType, Path.Combine(_path, $"{indexName}.{i.IndexType}.index")));
                }
                var indexBook = _indexBooks[indexName];
                i.IndexValue.ToBytes(indexBook.IndexType).IsValidIndexValue(i.IndexName, i.IndexValue, i.IndexType);
                indexBook.Add(i.IndexValue, position, length);
            }
        }

        protected void Insert(object key, Stream stream, params InsertIndexItem[] index){
            var keyIndexBook = _indexBooks["key"];
            var length = stream.Length;
            var position = _dataWR.Add(stream);
            if(position == -1)
                return;
            keyIndexBook.Add(key, position, (int)length);
            if(index == null || !index.Any())
                return;
            foreach(var i in index){
                i.IndexName.IsValidIndexName();
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName)){
                    i.IndexType.IsSupportedType(i.IndexName);
                    _indexBooks.TryAdd(indexName, new KeyIndexBook(indexName, i.IndexType, Path.Combine(_path, $"{indexName}.{i.IndexType}.index")));
                }
                var indexBook = _indexBooks[indexName];
                i.IndexValue.ToBytes(indexBook.IndexType).IsValidIndexValue(i.IndexName, i.IndexValue, i.IndexType);
                indexBook.Add(i.IndexValue, position, (int)length);
            }
        }

        public bool Any() => _indexBooks.Any() && ((IKeyValueIndexBook)_indexBooks["key"]).Any();

        public int Count() => !_indexBooks.Any() ? 0 : ((IKeyValueIndexBook)_indexBooks["key"]).Count();

        public string Get(object key){
            if(!_indexBooks.ContainsKey("key"))
                throw new Exceptions.KeyNotFoundException(key);
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);

            var indexItem = ((IKeyValueIndexBook)keyBook).Find(key) ?? throw new Exceptions.KeyNotFoundException(key);
            return _dataWR.Get(indexItem.ValuePosition, indexItem.ValueLength);
        }

        public Stream GetStream(object key){
            if(!_indexBooks.ContainsKey("key"))
                throw new Exceptions.KeyNotFoundException(key);
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);

            var indexItem = ((IKeyValueIndexBook)keyBook).Find(key) ?? throw new Exceptions.KeyNotFoundException(key);
            return _dataWR.GetStream(indexItem.ValuePosition, indexItem.ValueLength);
        }

        public bool Exists(object key){
            if(!_indexBooks.ContainsKey("key"))
                return false;
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);

            return ((IKeyValueIndexBook)keyBook).Exists(key);
        }

        public void Set(object key, string value, string keyType, params InsertIndexItem[] index)
        {
            if(!_indexBooks.ContainsKey("key")){
                keyType.IsSupportedKeyType("key");
                _indexBooks.TryAdd("key", new KeyValueIndexBook("key", keyType, Path.Combine(_path, $"key.{keyType}.index")));
            }
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);
            
            var indexItem = ((IKeyValueIndexBook)keyBook).DeleteIfExists(key);
            if(indexItem != null && indexItem.Deleted){
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
                var otherIndexBooks = _indexBooks.Values.Where(x => !x.IsKeyIndex);
                if(otherIndexBooks != null && otherIndexBooks.Any()){
                    foreach(var indexBook in otherIndexBooks){
                        indexBook.DeleteByPosition(indexItem.ValuePosition);
                    }
                }
            }
            Insert(key, value, index);
        }

        public void SetStream(object key, Stream stream, string keyType, params InsertIndexItem[] index)
        {
            if(!_indexBooks.ContainsKey("key")){
                keyType.IsSupportedKeyType("key");
                _indexBooks.TryAdd("key", new KeyValueIndexBook("key", keyType, Path.Combine(_path, $"key.{keyType}.index")));
            }
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);

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

        public void Delete(object key){
            if(!_indexBooks.ContainsKey("key"))
                throw new Exceptions.KeyNotFoundException(key);
            var keyBook = _indexBooks["key"];
            key.ToBytes(keyBook.IndexType).IsValidIndexValue(keyBook.IndexName, key, keyBook.IndexType);

            var indexItem = ((IKeyValueIndexBook)keyBook).Delete(key);
            if(indexItem == null)
                throw new Exceptions.KeyNotFoundException(key);
            if(indexItem.Deleted){
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
                var otherIndexBooks = _indexBooks.Values.Where(x => !x.IsKeyIndex);
                if(otherIndexBooks != null && otherIndexBooks.Any()){
                    foreach(var indexBook in otherIndexBooks){
                        indexBook.DeleteByPosition(indexItem.ValuePosition);
                    }
                }
            }
        }

        public void InsertHash(Dictionary<object, string> hash, string keyType, params InsertIndexHash[] index){
            if(!_indexBooks.ContainsKey("key"))
                _indexBooks.TryAdd("key", new KeyValueIndexBook("key", keyType, Path.Combine(_path, $"key.{keyType}.index")));
            var keyBook = _indexBooks["key"];
            var keyPositionList = _dataWR.FlatFileWR.AppendList(hash.ToArray()).ToArray();
            Func<object, string, int> calculateLength;
            if(keyType == typeof(string).Name)
                calculateLength = (key, type) => ((string)key).Length;//key.ToBytes(type).Length;
            else
                calculateLength = (key, type) => Converter.NUMBER_OF_BYTES[keyType];
            var keyIndexHash = keyPositionList.Select(pair => new {keyBytesLength = calculateLength(pair.Key, keyType), indexItemBytes = keyBook.GetBytes(pair.Key, pair.Value.Position, pair.Value.Length)})
                .GroupBy(obj => obj.keyBytesLength,
                        obj => obj.indexItemBytes,
                        (key, grp) => new KeyValuePair<int,byte[][]>(key, grp.ToArray())).ToDictionary(x => x.Key, x => x.Value);
            keyBook.InsertHash(keyIndexHash);
            if(index == null || !index.Any())
                return;
            foreach(var i in index){
                if(i.IndexType == typeof(string).Name)
                    calculateLength = (key, type) => ((string)key).Length;//key.ToBytes(type).Length;
                else
                    calculateLength = (key, type) => Converter.NUMBER_OF_BYTES[type];
                var indexName = i.IndexName.ToLower();
                if(!_indexBooks.ContainsKey(indexName))
                    _indexBooks.TryAdd(indexName, new KeyIndexBook(indexName, i.IndexType, Path.Combine(_path, $"{indexName}.{i.IndexType}.index")));
                var indexBook = _indexBooks[indexName];
                var indexHash = keyPositionList.Join(i.Hash, keyPosList => keyPosList.Key, hash => hash.Key, (keyPosList, hash) => new {key = hash.Value, val = keyPosList.Value})
                    .Select(indexItem => new {keyBytesLength = calculateLength(indexItem.key, i.IndexType), indexItemBytes = indexBook.GetBytes(indexItem.key, indexItem.val.Position, indexItem.val.Length)})
                    .GroupBy(obj => obj.keyBytesLength,
                            obj => obj.indexItemBytes,
                            (key, grp) => new KeyValuePair<int,byte[][]>(key, grp.ToArray())).ToDictionary(x => x.Key, x => x.Value);
                indexBook.InsertHash(indexHash);
            }
        }

        public PagedList<string> All(int page = 1, int pageSize = 10){
            if(!_indexBooks.ContainsKey("key"))
                return Enumerable.Empty<string>().ToPagedList(page, pageSize);
            var keyBook = (IKeyValueIndexBook)_indexBooks["key"];
            return keyBook.All().ToPagedList(page, pageSize).Read(_dataWR.FlatFileWR);
        }

        public PagedList<string> Filter(string indexName = null, CompareOperators? compareOperator = null, object value = null, int page = 1, int pageSize = 10){
            if(indexName == null || compareOperator == null)
                return All(page, pageSize);
            indexName.IsValidIndexName();
            var name = indexName.ToLower();
            if(!_indexBooks.ContainsKey(name))
                throw new IndexNotFoundException(indexName);
            var indexBook = _indexBooks[name];
            (value?.ToBytes(indexBook.IndexType) ?? new byte[0]).IsValidIndexValue(indexBook.IndexName, value, indexBook.IndexType);
            compareOperator.Value.IsValid(indexBook.IndexType);
            return indexBook.Filter(value, compareOperator.Value, page, pageSize).Read(_dataWR.FlatFileWR);
        }

        public int Count(string indexName = null, CompareOperators? compareOperator = null, object value = null){
            if(indexName == null || compareOperator == null)
                return Count();
            indexName.IsValidIndexName();
            var name = indexName.ToLower();
            if(!_indexBooks.ContainsKey(name))
                throw new IndexNotFoundException(indexName);
            var indexBook = _indexBooks[name];
            value.ToBytes(indexBook.IndexType).IsValidIndexValue(indexBook.IndexName, value, indexBook.IndexType);
            compareOperator.Value.IsValid(indexBook.IndexType);
            return indexBook.Count(value, compareOperator.Value);
        }

        public bool Any(string indexName = null, CompareOperators? compareOperator = null, object value = null){
            if(indexName == null || compareOperator == null)
                return Any();
            indexName.IsValidIndexName();
            var name = indexName.ToLower();
            if(!_indexBooks.ContainsKey(name))
                throw new IndexNotFoundException(indexName);
            var indexBook = _indexBooks[name];
            value.ToBytes(indexBook.IndexType).IsValidIndexValue(indexBook.IndexName, value, indexBook.IndexType);
            compareOperator.Value.IsValid(indexBook.IndexType);
            return indexBook.Any(value, compareOperator.Value);
        }

        public void Clear()
        {
            _dataWR.Clear();
            foreach(var book in _indexBooks.Values){
                book.Clear();
            }
        }

        public void Set(string key, string value)
        {
            Set(key, value, typeof(string).Name);
        }

        public void SetStream(string key, Stream value)
        {
            SetStream(key, value, typeof(string).Name);
        }

        public void Delete()
        {
            Directory.Delete(_path, true);
        }
    }
}