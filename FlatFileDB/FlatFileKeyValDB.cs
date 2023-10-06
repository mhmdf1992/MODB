using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDB : FlatFileKeyValDBBase, IKeyValDB
    {

        public FlatFileKeyValDB(string path, int? numberOfManifestFiles = 10, DBStatus status = DBStatus.READY): base(path, numberOfManifestFiles ?? 10, status){
        }
        public void Delete(string key){
            Validator.ValidateKey(key);
            if(!ManifestContainsItemToDelete(key, out ManifestItemMinToDel? manifestItem))
                throw new Exceptions.KeyNotFoundException(key);
            _manFileWRs[manifestItem.Value.Manifest].Remove(manifestItem.Value.DelPosition);
        }

        public string Get(string key){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin? manifestItem))
                throw new Exceptions.KeyNotFoundException(key);
            return _flatFileWR.Read(manifestItem.Value.Position, manifestItem.Value.Length);
        }

        public bool Get(string key, Action<Stream> action){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin? manifestItem))
                return false;
            _flatFileWR.ReadStream(action, manifestItem.Value.Position, manifestItem.Value.Length);
            return true;
        }

        public void Insert(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var manfile = GetManifestWR();
            var manifestItem = new ManifestItem(key, position, val.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), manfile.Key, noTags ? "" : string.Join(' ', tags));
            (manfile.Value as IFileWR).WriteAtEnd(manifestItem.ToCsv());
        }

        public void Insert(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var manfile = GetManifestWR();
            var manifestItem = new ManifestItem(key, position, (int)stream.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), manfile.Key, noTags ? "" : string.Join(' ', tags));
            (manfile.Value as IFileWR).WriteAtEndStream(manifestItem.ToCsvStream());
        }

        public void Set(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItemToDelete(key, out ManifestItemMinToDel? manifestItem)){
                _manFileWRs[manifestItem.Value.Manifest].Remove(manifestItem.Value.DelPosition);
            }
            Insert(key, val, tags, timeStamp);
        }

        public void Set(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItemToDelete(key, out ManifestItemMinToDel? manifestItem)){
                _manFileWRs[manifestItem.Value.Manifest].Remove(manifestItem.Value.DelPosition);
            }
            Insert(key, stream, tags, timeStamp);
        }

        public PagedList<string> GetTags(string text = null, int page = 1, int pageSize = 10){
            return Task.WhenAll(_manFileWRs.Values.Select( x => Task.Run(() => x.GetTags(text)))).Result.SelectMany(x => x).SelectMany(x => x)
                .DefaultIfEmpty()
                .Distinct()
                .ToPagedList(page, pageSize);
        }

        public bool Exists(string key) => Validator.ValidateKey(key) ? ManifestContainsItem(key, out ManifestItemMin? manifestItem) : false;

        public PagedList<string> Get(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, int page = 1, int pageSize = 10){
            return Task.WhenAll(_manFileWRs.Values.Select(x => Task.Run(() => x.FilterMin(tags, timeStampFrom, timeStampTo)))).Result
                .DefaultIfEmpty()
                .SelectMany(x => x)
                .ToPagedList(page, pageSize)
                .Read(_flatFileWR);
        }

        public PagedList<string> GetKeys(int page = 1, int pageSize = 10)
        {
            return Task.WhenAll(_manFileWRs.Values.Select(x => Task.Run(() => x.GetKeys()))).Result
                .DefaultIfEmpty()
                .SelectMany(x => x)
                .ToPagedList(page, pageSize);
        }

        public PagedList<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, int page = 1, int pageSize = 10)
        {
            return Task.WhenAll(_manFileWRs.Values.Select(x => Task.Run(() => x.GetKeys(tags, timeStampFrom, timeStampTo)))).Result
                .DefaultIfEmpty()
                .SelectMany(x => x)
                .ToPagedList(page, pageSize);
        }

        public void Clone(IKeyValDB cloneDb)
        {
            foreach(IFileWR man in _manFileWRs.Values){
                using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(man.FileInfo.FullName, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                    while(csv.Read())
                    {
                        if(csv.GetInt16(5) == 0)
                            cloneDb.Set(csv.GetString(0), _flatFileWR.Read(csv.GetInt64(1), csv.GetInt32(2)), csv.GetString(4).Split(' '), csv.GetInt64(3));
                    }
                }
            }
        }

        public void Rename(string name)
        {
            foreach(var file in Directory.GetFiles(_path)){
                var destination = file.Replace(this._name, name);
                new FileInfo(destination).Directory.Create();
                File.Move(file, destination);
            }
        }

        public void Delete()
        {
            Directory.Delete(this._path, true);
        }
    }
}