using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDB : FlatFileKeyValDBBase, IKeyValDB
    {
        public FlatFileKeyValDB(string path, int numberOfManifestFiles = 10): base(path, numberOfManifestFiles){

        }
        public void Delete(string key){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                throw new Exceptions.KeyNotFoundException(key);
            ManifestRewriteRemoveKey(key, manFileWR);
        }

        public string Get(string key){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                throw new Exceptions.KeyNotFoundException(key);
            return _flatFileWR.Read(manifestItem.Position, manifestItem.Length);
        }

        public bool Get(string key, Action<Stream> action){
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                return false;
            _flatFileWR.ReadStream(action, manifestItem.Position, manifestItem.Length);
            return true;
        }

        public void Insert(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var manifestItem = new ManifestItem(key, position, val.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), noTags ? "" : string.Join(' ', tags));
            GetManifestWR().WriteAtEnd(manifestItem.ToCsv());
        }

        public void Insert(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var manifestItem = new ManifestItem(key, position, (int)stream.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), noTags ? "" : string.Join(' ', tags));
            GetManifestWR().WriteAtEndStream(manifestItem.ToCsvStream());
        }

        public void Update(ManifestItemMin manifestItem, IFileWR manFileWR, string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var newManifestItem = new ManifestItem(key, position, val.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), noTags ? "" : string.Join(' ', tags));
            ManifestRewriteUpdateKey(key, newManifestItem, manFileWR);
        }

        public void Update(ManifestItemMin manifestItem, IFileWR manFileWR, string key, Stream stream, IEnumerable<string> tags = null, long ? timeStamp = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var noTags = tags == null || !tags.Any();
            var newManifestItem = new ManifestItem(key, position, (int)stream.Length, timeStamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds(), noTags ? "" : string.Join(' ', tags));
            ManifestRewriteUpdateKey(key, newManifestItem, manFileWR);
        }

        public void Set(string key, string val, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR)){
                Update(manifestItem, manFileWR, key, val, tags, timeStamp);
                return;
            }
            Insert(key, val, tags, timeStamp);
        }

        public void Set(string key, Stream stream, IEnumerable<string> tags = null, long? timeStamp = null){
            Validator.ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR)){
                Update(manifestItem, manFileWR, key, stream, tags, timeStamp);
                return;
            }
            Insert(key, stream, tags, timeStamp);
        }

        public PagedList<string> GetTags(bool? orderAsc = null, bool? orderDesc = null, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsTags(x), new System.Threading.CancellationTokenSource(5000).Token))).Result;
            if(res == null || !res.Any())
                Enumerable.Empty<string>().ToPagedList(page, pageSize);
            if(orderAsc == true)
                return res.SelectMany(x => x.Item1)
                    .Distinct()
                    .OrderBy(x => x)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderDesc == true)
                return res.SelectMany(x => x.Item1)
                    .Distinct()
                    .OrderByDescending(x => x)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            return res.SelectMany(x => x.Item1)
                .Distinct()
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
        }

        public PagedList<string> GetByKeyRegexPattern(string keyRegexPattern, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsByKeyPattern(keyRegexPattern, x), new System.Threading.CancellationTokenSource(5000).Token))).Result;
            if(res == null || !res.Any())
                return Enumerable.Empty<string>().ToPagedList(page, pageSize);
            return res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return new ManifestItemMin(Helper.ConvertToLong(csvArr[1].ToArray()), Helper.ConvertToInt(csvArr[2].ToArray()));
                }).Select(x => _flatFileWR.Read(x.Position, x.Length))
                .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
        }

        public bool Exists(string key) => Validator.ValidateKey(key) ? ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR) : false;

        public PagedList<string> GetKeys(int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsAll(x), new System.Threading.CancellationTokenSource(5000).Token))).Result;
            if(res == null || !res.Any())
                return Enumerable.Empty<string>().ToPagedList(page, pageSize);
            return res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return csvArr[0];
                }).ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
        }

        public PagedList<string> Get(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, bool? orderByKeyAsc = null, bool? orderByKeyDesc = null, bool? orderByTimeStampAsc = null, bool? orderByTimeStampDesc = null, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FilterManifestCsvRecords(x, tags, timeStampFrom, timeStampTo), new System.Threading.CancellationTokenSource(5000).Token))).Result;
            if(res == null || !res.Any())
                Enumerable.Empty<string>().ToPagedList(page, pageSize);
            if(orderByKeyAsc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderBy(man => man.Key)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(man => _flatFileWR.Read(man.Position, man.Length))
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByKeyDesc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderByDescending(man => man.Key)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(man => _flatFileWR.Read(man.Position, man.Length))
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByTimeStampAsc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderBy(man => man.timeStamp)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(man => _flatFileWR.Read(man.Position, man.Length))
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByTimeStampDesc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderByDescending(man => man.timeStamp)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(man => _flatFileWR.Read(man.Position, man.Length))
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            return res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(man => _flatFileWR.Read(man.Position, man.Length))
                .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            
        }

        public PagedList<MODBRecord> GetDetailed(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null, bool? orderByKeyAsc = null, bool? orderByKeyDesc = null, bool? orderByTimeStampAsc = null, bool? orderByTimeStampDesc = null, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FilterManifestCsvRecords(x, tags, timeStampFrom, timeStampTo), new System.Threading.CancellationTokenSource(5000).Token))).Result;
            if(res == null || !res.Any())
                Enumerable.Empty<MODBRecord>().ToPagedList(page, pageSize);
            if(orderByKeyAsc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderBy(x => x.Key)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(x => new MODBRecord(){
                        Key = x.Key, 
                        Value = _flatFileWR.Read(x.Position, x.Length),
                        TimeStamp = x.timeStamp,
                        Tags = x.Tags.Split(' ')
                        })
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByKeyDesc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderByDescending(x => x.Key)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(x => new MODBRecord(){
                        Key = x.Key, 
                        Value = _flatFileWR.Read(x.Position, x.Length),
                        TimeStamp = x.timeStamp,
                        Tags = x.Tags.Split(' ')
                        })
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByTimeStampAsc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderBy(x => x.timeStamp)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(x => new MODBRecord(){
                        Key = x.Key, 
                        Value = _flatFileWR.Read(x.Position, x.Length),
                        TimeStamp = x.timeStamp,
                        Tags = x.Tags.Split(' ')
                        })
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            if(orderByTimeStampDesc == true)
                return res.SelectMany(x => x.Item1)
                    .OrderByDescending(x => x.timeStamp)
                    .Skip((page * pageSize) - pageSize)
                    .Take(pageSize)
                    .Select(x => new MODBRecord(){
                        Key = x.Key, 
                        Value = _flatFileWR.Read(x.Position, x.Length),
                        TimeStamp = x.timeStamp,
                        Tags = x.Tags.Split(' ')
                        })
                    .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
            return res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => new MODBRecord(){
                    Key = x.Key, 
                    Value = _flatFileWR.Read(x.Position, x.Length),
                    TimeStamp = x.timeStamp,
                    Tags = x.Tags.Split(' ')
                    })
                .ToPagedList(page, pageSize, res.Sum(x => x.Item1.Count()));
        }

        public MODBRecord GetDetailed(string key)
        {
            Validator.ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItem manifestItem, out IFileWR manFileWR))
                throw new Exceptions.KeyNotFoundException(key);
            return new MODBRecord(){ 
                Key = manifestItem.Key,
                Value = _flatFileWR.Read(manifestItem.Position, manifestItem.Length),
                TimeStamp = manifestItem.timeStamp,
                Tags = manifestItem.Tags.Split(' ')
            };
        }
    }
}