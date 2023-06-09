using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDB : IKeyValDB
    {
        readonly char[] KEY_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-".ToArray();
        protected string _path;
        protected string _flatFilePath;
        protected string _tagsFilePath;
        protected IFileWR _flatFileWR;
        protected IFileWR _tagsFileWR; 
        protected List<IFileWR> _manFileWRs;
        protected int _numberOfManFiles;
        protected string _name;
        public string Name => _name;
        public long Size => _flatFileWR.Size + _manFileWRs.Sum(x => x.Size) + _tagsFileWR.Size;
        public FlatFileKeyValDB(string path, int numberOfManifestFiles = 10){
            _path = path;
            _numberOfManFiles = numberOfManifestFiles;
            try{
                var name = Path.GetFileName(_path);
                _name = name;
                var _flatFilePath = Path.Combine(path, $"{name}.dat");
                var _tagsFilePath = Path.Combine(path, $"{name}.tag");
                _flatFileWR = new ConcurrentFileWR(_flatFilePath);
                _tagsFileWR = new ConcurrentFileWR(_tagsFilePath);
                _manFileWRs = new List<IFileWR>();
                LoadManifests();
            }catch{throw;}
        }

        void LoadManifests(){
            var name = Path.GetFileName(_path);
            var files = Directory.GetFiles(_path, "*.man");
            if(files == null || !files.Any()){
                for(int i = 1; i <= _numberOfManFiles; i ++){
                    _manFileWRs.Add(new ConcurrentFileWR(Path.Combine(_path, $"{name}({i}).man")));
                }
                return;
            }
            files.ToList().ForEach(path => _manFileWRs.Add(new ConcurrentFileWR(path)));
        }

        IFileWR GetManifestWR() {
            var manFileWR = _manFileWRs.Select(x => new {FileWR = x, Order = x.Size}).OrderBy(x => x.Order).Select(x => x.FileWR).FirstOrDefault();
            if(manFileWR == null){
                var name = Path.GetFileName(_path);
                manFileWR = new ConcurrentFileWR(Path.Combine(_path, $"{name}({_manFileWRs.Count + 1}).man"));
                _manFileWRs.Add(manFileWR);
            }
            return manFileWR;
        }

        Tuple<string,IFileWR> FindManifestCsvRecord(string key, IFileWR manFileWR){
                var csvRecord = "";
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var line = reader.ReadLine();
                            var lineKey = line.Split(',')[0];
                            if(lineKey == key){
                                csvRecord = line;
                                return;
                            }
                        }
                    }
                });
                return new Tuple<string,IFileWR>(csvRecord, manFileWR);
            }

        Task<Tuple<IEnumerable<string>, int, IFileWR>> FindManifestCsvRecordsByTags(IEnumerable<string> tags, IFileWR manFileWR) => Task.Run(() => {
                var csvRecords = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var line = reader.ReadLine();
                            var tagsString = line.Split(',')[4];
                            var tagsList = tagsString.Split(' ');
                            if(tagsList.Any(x => tags.Any(y => y == x)))
                                csvRecords.Add(line);
                        }
                    }
                });
                return new Tuple<IEnumerable<string>, int, IFileWR>(csvRecords, csvRecords.Count, manFileWR);
            });

        Task<Tuple<IEnumerable<string>, int, IFileWR>> FindManifestCsvRecordsByKeyPattern(string keyRegexPattern, IFileWR manFileWR) => Task.Run(() => {
                var csvRecords = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var line = reader.ReadLine();
                            var key = line.Split(',')[0];
                            if(System.Text.RegularExpressions.Regex.IsMatch(key, keyRegexPattern))
                                csvRecords.Add(line);
                        }
                    }
                });
                return new Tuple<IEnumerable<string>, int, IFileWR>(csvRecords, csvRecords.Count, manFileWR);
            });
        bool ManifestContainsItem(string key, out ManifestItemMin manifestItem, out IFileWR manFileWR){
            manifestItem = default;
            manFileWR = default;
            var res = Task.WhenAll(_manFileWRs.Select((x) => Task.Run(() => FindManifestCsvRecord(key, x)))).Result;
            if(!res.Any(x => !string.IsNullOrEmpty(x.Item1)))
                return false;
            var resTuple = res.First(x => !string.IsNullOrEmpty(x.Item1));
            var csvRecord = resTuple.Item1;
            var csvArr = csvRecord.Split(',');
            manifestItem = new ManifestItemMin(ConvertToLong(csvArr[1].ToArray()), ConvertToInt(csvArr[2].ToArray()));
            manFileWR = resTuple.Item2;
            return true;
        }

        Task<Tuple<IEnumerable<string>, int, IFileWR>> FindManifestCsvRecordsAll(IFileWR manFileWR) => Task.Run(() => {
                var csvRecords = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            csvRecords.Add(reader.ReadLine());
                        }
                    }
                });
                return new Tuple<IEnumerable<string>, int,IFileWR>(csvRecords, csvRecords.Count, manFileWR);
            });

        Task<Tuple<IEnumerable<string>, int, IFileWR>> FindManifestCsvRecordsByDateRange(DateTime from, DateTime to, IFileWR manFileWR) => Task.Run(() => {
                var csvRecords = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var line = reader.ReadLine();
                            var ticksString = line.Split(',')[3];
                            var ticks = ConvertToLong(ticksString.ToArray());
                            if(ticks > from.Ticks && ticks < to.Ticks)
                                csvRecords.Add(reader.ReadLine());
                        }
                    }
                });
                return new Tuple<IEnumerable<string>, int, IFileWR>(csvRecords, csvRecords.Count, manFileWR);
            });


        void ManifestRewriteRemoveKey(string key, IFileWR manFileWR){
            var resultText = System.Text.RegularExpressions.Regex.Replace(manFileWR.Read(), $"^{key},([0-9]+),([0-9]+),.*\n", string.Empty, System.Text.RegularExpressions.RegexOptions.Multiline);
            manFileWR.Clear();
            manFileWR.Write(resultText);
        }

        void ManifestRewriteUpdateKey(string key, ManifestItem newManItem, IFileWR manFileWR){
            var resultText = System.Text.RegularExpressions.Regex.Replace(manFileWR.Read(), $"^{key},([0-9]+),([0-9]+),.*\n", newManItem.ToCsv(), System.Text.RegularExpressions.RegexOptions.Multiline);
            manFileWR.Clear();
            manFileWR.Write(resultText);
        }

        void TagsRemoveTags(IEnumerable<string> tags){
            if(tags == null || !tags.Any())
                return;
            var findTagPattern = tags.Select(x => $"{x},|{x}$");
            var resultText = System.Text.RegularExpressions.Regex.Replace(_tagsFileWR.Read(), $"({string.Join('|', findTagPattern)})", string.Empty, System.Text.RegularExpressions.RegexOptions.Multiline);
            _tagsFileWR.Clear();
            _tagsFileWR.Write(resultText);
        }

        void TagsAddTags(IEnumerable<string> tags){
            if(tags == null || !tags.Any())
                return;
            var tagsToWrite = new List<string>();
            var tagsText = _tagsFileWR.Read();
            var tagsList = tagsText.Split(',');
            foreach(var tag in tags){
                if(!tagsList.Any(x => x == tag))
                    tagsToWrite.Add(tag);
            }
            if(tagsToWrite.Any())
                _tagsFileWR.WriteAtEnd($"{(_tagsFileWR.FileInfo.Length > 0 ? "," : "")}{string.Join(',', tagsToWrite)}");
        }

        long ConvertToLong(char[] str){
            int i=0;
            long sum=0;
            while(i < str.Length && str[i]!='\0')
            {
                if(str[i]< 48 || str[i] > 57)
                    throw new Exception($"Unable to convert it into integer ({str[i]})");
                else
                {
                    sum = sum*10 + (str[i] - 48);
                    i++;
                }
            }
            return sum;
        }

        int ConvertToInt(char[] str){
            int i=0,sum=0;
            while(i < str.Length && str[i]!='\0')
            {
                if(str[i]< 48 || str[i] > 57)
                    throw new Exception($"Unable to convert it into integer ({str[i]})");
                else
                {
                    sum = sum*10 + (str[i] - 48);
                    i++;
                }
            }
            return sum;
        }
        
        bool ValidateKey(string key) => string.IsNullOrEmpty(key) || key.Any(x => !KEY_ALLOWED_CHARS.Contains(x)) ? throw new ArgumentException($"{key} is not a valid key. keys must match ^[a-zA-Z0-9_.-]+$", nameof(key)) : true;

        public void Delete(string key){
            ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                throw new Exceptions.KeyNotFoundException(key);
            ManifestRewriteRemoveKey(key, manFileWR);
        }

        public string Get(string key){
            ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                throw new Exceptions.KeyNotFoundException(key);
            return _flatFileWR.Read(manifestItem.Position, manifestItem.Length);
        }

        public bool Get(string key, Action<Stream> action){
            ValidateKey(key);
            if(!ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR))
                return false;
            _flatFileWR.ReadStream(action, manifestItem.Position, manifestItem.Length);
            return true;
        }

        public void Insert(string key, string val, IEnumerable<string> tags = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var manifestItem = new ManifestItem(key, position, val.Length, DateTime.UtcNow.Ticks, tags == null || !tags.Any() ? "" : string.Join(' ', tags));
            GetManifestWR().WriteAtEnd(manifestItem.ToCsv());
            if(tags != null && tags.Any())
                TagsAddTags(tags);
        }

        public void Insert(string key, Stream stream, IEnumerable<string> tags = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var manifestItem = new ManifestItem(key, position, (int)stream.Length, DateTime.UtcNow.Ticks, tags == null || !tags.Any() ? "" : string.Join(' ', tags));
            GetManifestWR().WriteAtEndStream(manifestItem.ToCsvStream());
            if(tags != null && tags.Any())
                TagsAddTags(tags);
        }

        public void Update(ManifestItemMin manifestItem, IFileWR manFileWR, string key, string val, IEnumerable<string> tags = null){
            var position = _flatFileWR.WriteAtEnd(val);
            if(position == -1)
                return;
            var newManifestItem = new ManifestItem(key, position, val.Length, DateTime.UtcNow.Ticks, tags == null || !tags.Any() ? "" : string.Join(' ', tags));
            ManifestRewriteUpdateKey(key, newManifestItem, manFileWR);
            if(tags != null && tags.Any())
                TagsAddTags(tags);
        }

        public void Update(ManifestItemMin manifestItem, IFileWR manFileWR, string key, Stream stream, IEnumerable<string> tags = null){
            var position = _flatFileWR.WriteAtEndStream(stream);
            if(position == -1)
                return;
            var newManifestItem = new ManifestItem(key, position, (int)stream.Length, DateTime.UtcNow.Ticks, tags == null || !tags.Any() ? "" : string.Join(' ', tags));
            ManifestRewriteUpdateKey(key, newManifestItem, manFileWR);
            if(tags != null && tags.Any())
                TagsAddTags(tags);
        }

        public void Set(string key, string val, IEnumerable<string> tags = null){
            ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR)){
                Update(manifestItem, manFileWR, key, val, tags);
                return;
            }
            Insert(key, val, tags);
        }

        public void Set(string key, Stream stream, IEnumerable<string> tags = null){
            ValidateKey(key);
            if(ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR)){
                Update(manifestItem, manFileWR, key, stream, tags);
                return;
            }
            Insert(key, stream, tags);
        }

        public PagedList<string> GetTags(int page = 1, int pageSize = 10){
            var tags = _tagsFileWR.Read();
            if(string.IsNullOrEmpty(tags))
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = tags.Split(',');
            return new  PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)list.Count() / pageSize),
                Items = list};
        }

        public PagedList<string> GetByTags(IEnumerable<string> tags, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsByTags(tags, x)))).Result;
            if(res == null || !res.Any())
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return new ManifestItemMin(ConvertToLong(csvArr[1].ToArray()), ConvertToInt(csvArr[2].ToArray()));
                }).Select(x => _flatFileWR.Read(x.Position, x.Length));
            return new  PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)res.Sum(x => x.Item2) / pageSize),
                Items = list};
        }

        public PagedList<string> GetByKeyRegexPattern(string keyRegexPattern, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsByKeyPattern(keyRegexPattern, x)))).Result;
            if(res == null || !res.Any())
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return new ManifestItemMin(ConvertToLong(csvArr[1].ToArray()), ConvertToInt(csvArr[2].ToArray()));
                }).Select(x => _flatFileWR.Read(x.Position, x.Length));
            return new  PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)res.Sum(x => x.Item2) / pageSize),
                Items = list};
        }

        public PagedList<string> GetAll(int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsAll(x)))).Result;
            if(res == null || !res.Any())
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return new ManifestItemMin(ConvertToLong(csvArr[1].ToArray()), ConvertToInt(csvArr[2].ToArray()));
                }).Select(x => _flatFileWR.Read(x.Position, x.Length));
            return new  PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)res.Sum(x => x.Item2) / pageSize),
                Items = list};
        }

        public PagedList<string> GetByDateRange(DateTime from, DateTime? to = null, int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsByDateRange(from, to ?? DateTime.UtcNow, x)))).Result;
            if(res == null || !res.Any())
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return new ManifestItemMin(ConvertToLong(csvArr[1].ToArray()), ConvertToInt(csvArr[2].ToArray()));
                }).Select(x => _flatFileWR.Read(x.Position, x.Length));
            return new  PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)res.Sum(x => x.Item2) / pageSize),
                Items = list};
        }

        public bool Exists(string key) => ValidateKey(key) ? ManifestContainsItem(key, out ManifestItemMin manifestItem, out IFileWR manFileWR) : false;

        public PagedList<string> GetKeys(int page = 1, int pageSize = 10){
            var res = Task.WhenAll(_manFileWRs.Select( x => Task.Run(() => FindManifestCsvRecordsAll(x)))).Result;
            if(res == null || !res.Any())
                return new PagedList<string>(){ Page = page, PageSize = 0, Items = Enumerable.Empty<string>()};
            var list = res.SelectMany(x => x.Item1)
                .Skip((page * pageSize) - pageSize)
                .Take(pageSize)
                .Select(x => {
                    var csvArr = x.Split(',');
                    return csvArr[0];
                });
            return new PagedList<string>(){ 
                Page = page, 
                PageSize = pageSize, 
                TotalPages = (int)Math.Ceiling((double)res.Sum(x => x.Item2) / pageSize),
                Items = list};
        }

        public class ManifestItem{
            long _ticks;
            public long ticks => _ticks;
            string _tags;
            public string Tags => _tags;
            string _key;
            long _position;
            int _length;
            public string Key => _key;
            public long Position => _position;
            public int Length => _length;
            
            public ManifestItem(string key, long pos, int len, long ticks, string tags = null){
                _key = key;
                _position = pos;
                _length = len;
                _ticks = ticks;
                _tags = tags;
            }
            public string ToCsv() => $"{_key},{_position},{_length},{_ticks},{(string.IsNullOrEmpty(_tags) ? "" : _tags)}{Environment.NewLine}";
            public Stream ToCsvStream() => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ToCsv()));
        }
        public struct ManifestItemMin{
            public ManifestItemMin(long pos, int len){
                _position = pos;
                _length = len;
            }
            long _position;
            public long Position => _position;
            int _length;
            public int Length => _length;
        }
    }
}