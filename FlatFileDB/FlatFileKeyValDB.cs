using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDB : FlatFileKeyValDBBase, IKeyValDB
    {
        public FlatFileKeyValDB(string path, int? numberOfManifestFiles = 10): base(path, numberOfManifestFiles ?? 10){
        }
        public void Delete(string key){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin? manifestItem))
                throw new Exceptions.KeyNotFoundException(key);
            _manFileWRs[manifestItem.Value.Manifest].Remove(key);
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

        public void Update(KeyValuePair<int,IManifestCSVFile> manFileWR, string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var newManifestItem = new ManifestItem(key, position, val.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), manFileWR.Key, noTags ? "" : string.Join(' ', tags));
            manFileWR.Value.Update(key, newManifestItem);
        }

        public void Update(KeyValuePair<int,IManifestCSVFile> manFileWR, string key, Stream stream, IEnumerable<string> tags = null, long ? timeStamp = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var newManifestItem = new ManifestItem(key, position, (int)stream.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), manFileWR.Key, noTags ? "" : string.Join(' ', tags));
            manFileWR.Value.Update(key, newManifestItem);
        }

        public void Set(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin? manifestItem)){
                Update(new KeyValuePair<int, IManifestCSVFile>(manifestItem.Value.Manifest, _manFileWRs[manifestItem.Value.Manifest]), key, val, tags, timeStamp);
                return;
            }
            Insert(key, val, tags, timeStamp);
        }

        public void Set(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin? manifestItem)){
                Update(new KeyValuePair<int, IManifestCSVFile>(manifestItem.Value.Manifest, _manFileWRs[manifestItem.Value.Manifest]), key, stream, tags, timeStamp);
                return;
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
    }
}