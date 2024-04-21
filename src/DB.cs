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

        protected void Insert(string key, string value, params KeyValuePair<string, string>[] index){
            var keyIndexBook = _indexBooks["key"];
            var position = _dataWR.Add(value);
            if(position == -1)
                return;
            var length = _dataWR.Encoding.GetByteCount(value);
            keyIndexBook.Add(key, position, length);
        }

        protected void Insert(string key, Stream stream, params KeyValuePair<string, string>[] index){
            var keyIndexBook = _indexBooks["key"];
            var length = stream.Length;
            var position = _dataWR.Add(stream);
            if(position == -1)
                return;
            keyIndexBook.Add(key, position, (int)length);
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

        public void Set(string key, string value, params KeyValuePair<string, string>[] index)
        {
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).DeleteIfExists(key);
            if(indexItem != null && indexItem.Deleted)
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
            Insert(key, value, index);
        }

        public void SetStream(string key, Stream stream, params KeyValuePair<string, string>[] index)
        {
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).DeleteIfExists(key);
            if(indexItem != null && indexItem.Deleted)
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
            Insert(key, stream, index);
        }

        public void Delete(string key){
            var keyBook = _indexBooks["key"];
            Validator.ValidateKey(key, keyBook.KeyMaxBytes);
            var indexItem = ((IKeyValueIndexBook)keyBook).Delete(key);
            if(indexItem.Deleted)
                _dataWR.Erase(indexItem.ValuePosition, indexItem.ValueLength);
        }

        public void InsertHash(Dictionary<string, string> hash){
            var keyBook = _indexBooks["key"];
            foreach(var pair in hash){
                Validator.ValidateKey(pair.Key, keyBook.KeyMaxBytes);
            }
            var result = _dataWR.FlatFileWR.AppendList(hash.ToArray()).ToArray();
            var indexHash = result.Select(pair => new {keyBytesLength = Encoding.UTF8.GetByteCount(pair.Key), indexItemBytes = keyBook.GetBytes(pair.Key, pair.Value.Position, pair.Value.Length)})
                .GroupBy(obj => obj.keyBytesLength,
                        obj => obj.indexItemBytes,
                        (key, grp) => new KeyValuePair<int,byte[][]>(key, grp.ToArray())).ToDictionary(x => x.Key, x => x.Value);
            keyBook.InsertHash(indexHash);
        }

        public PagedList<string> All(int page = 1, int pageSize = 10) => ((IKeyValueIndexBook)_indexBooks["key"]).All().ToPagedList(page, pageSize).Read(_dataWR.FlatFileWR);

        public void Clear()
        {
            foreach(var book in _indexBooks.Values){
                book.Clear();
            }
        }
    }
}