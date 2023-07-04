using MODB.ConcurrentFile;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDBBase{
        protected string _path;
        protected string _flatFilePath;
        protected IFileWR _flatFileWR;
        protected List<IFileWR> _manFileWRs;
        protected int _numberOfManFiles;
        protected string _name;
        public string Name => _name;
        public long Size => _flatFileWR.Size + _manFileWRs.Sum(x => x.Size);
        public FlatFileKeyValDBBase(string path, int numberOfManifestFiles = 10){
            _path = path;
            _numberOfManFiles = numberOfManifestFiles;
            try{
                var name = Path.GetFileName(_path);
                _name = name;
                var _flatFilePath = Path.Combine(path, $"{name}.dat");
                _flatFileWR = new ConcurrentFileWR(_flatFilePath);
                _manFileWRs = new List<IFileWR>();
                LoadManifests();
            }catch{throw;}
        }

        protected void LoadManifests(){
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

        protected IFileWR GetManifestWR() {
            var manFileWR = _manFileWRs.Select(x => new {FileWR = x, Order = x.Size}).OrderBy(x => x.Order).Select(x => x.FileWR).FirstOrDefault();
            if(manFileWR == null){
                var name = Path.GetFileName(_path);
                manFileWR = new ConcurrentFileWR(Path.Combine(_path, $"{name}({_manFileWRs.Count + 1}).man"));
                _manFileWRs.Add(manFileWR);
            }
            return manFileWR;
        }

        protected Tuple<ManifestItemMin?,IFileWR> FindManifestMinCsvRecord(string key, IFileWR manFileWR, System.Threading.CancellationTokenSource cs){
                ManifestItemMin? manifestItem = null;
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream  && ! cs.IsCancellationRequested){
                            var lineParams = reader.ReadLine().Split(',');
                            var k = lineParams[0];
                            if(k == key){
                                var position = Helper.ConvertToLong(lineParams[1].ToArray());
                                var length = Helper.ConvertToInt(lineParams[2].ToArray());
                                manifestItem = new ManifestItemMin(position, length);
                                cs.Cancel();
                                return;
                            }
                        }
                    }
                });
                return new Tuple<ManifestItemMin?,IFileWR>(manifestItem, manFileWR);
            }
        
        protected Tuple<ManifestItem?,IFileWR> FindManifestCsvRecord(string key, IFileWR manFileWR, System.Threading.CancellationTokenSource cs){
                ManifestItem? manifestItem = null;
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream && ! cs.IsCancellationRequested){
                            var lineParams = reader.ReadLine().Split(',');
                            var k = lineParams[0];
                            if(k == key){
                                var position = Helper.ConvertToLong(lineParams[1].ToArray());
                                var length = Helper.ConvertToInt(lineParams[2].ToArray());
                                var timeStamp = Helper.ConvertToLong(lineParams[3].ToArray());
                                var tags = lineParams[4];
                                manifestItem = new ManifestItem(k, position, length, timeStamp, tags);
                                cs.Cancel();
                                return;
                            }
                        }
                    }
                });
                return new Tuple<ManifestItem?,IFileWR>(manifestItem, manFileWR);
            }

        protected Task<Tuple<IEnumerable<ManifestItem>, IFileWR>> FilterManifestCsvRecords(IFileWR manFileWR, IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null) => Task.Run(() => {
                
                var csvRecords = new List<ManifestItem>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var lineParams = reader.ReadLine().Split(',');
                            var key = lineParams[0];
                            var positionString = lineParams[1];
                            var position = Helper.ConvertToLong(positionString.ToArray());
                            var lengthString = lineParams[2];
                            var length = Helper.ConvertToInt(lengthString.ToArray());
                            var timeStampString = lineParams[3];
                            var timeStamp = Helper.ConvertToLong(timeStampString.ToArray());
                            var tagsString = lineParams[4];
                            var tagsList = tagsString.Split(' ');
                            if(((tags == null || !tags.Any()) || tagsList.Any(x => tags.Any(y => y == x))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                                csvRecords.Add(new ManifestItem(key, position, length, timeStamp, tagsString) );
                        }
                    }
                });
                return new Tuple<IEnumerable<ManifestItem>, IFileWR>(csvRecords, manFileWR);
            });
        
        protected Task<Tuple<IEnumerable<string>, IFileWR>> FindManifestCsvRecordsTags(IFileWR manFileWR) => Task.Run(() => {
                var tags = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            var tagsString = reader.ReadLine().Split(',')[4];
                            if(!string.IsNullOrEmpty(tagsString)){
                                tags.AddRange(tagsString.Split(' '));
                            }
                        }
                    }
                });
                return new Tuple<IEnumerable<string>, IFileWR>(tags, manFileWR);
            });

        protected Task<Tuple<IEnumerable<string>, int, IFileWR>> FindManifestCsvRecordsByKeyPattern(string keyRegexPattern, IFileWR manFileWR) => Task.Run(() => {
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
        protected bool ManifestContainsItem(string key, out ManifestItemMin manifestItem, out IFileWR manFileWR){
            manifestItem = default;
            manFileWR = default;
            var res = Task.WhenAll(_manFileWRs.Select((x) => Task.Run(() => FindManifestMinCsvRecord(key, x, new System.Threading.CancellationTokenSource())))).Result;
            if(!res.Any(x => x.Item1 != null))
                return false;
            var resTuple = res.First(x => x.Item1 != null);
            manifestItem = resTuple.Item1.Value;
            manFileWR = resTuple.Item2;
            return true;
        }

        protected bool ManifestContainsItem(string key, out ManifestItem manifestItem, out IFileWR manFileWR){
            manifestItem = default;
            manFileWR = default;
            var res = Task.WhenAll(_manFileWRs.Select((x) => Task.Run(() => FindManifestCsvRecord(key, x, new System.Threading.CancellationTokenSource())))).Result;
            if(!res.Any(x => x.Item1 != null))
                return false;
            var resTuple = res.First(x => x.Item1 != null);
            manifestItem = resTuple.Item1.Value;
            manFileWR = resTuple.Item2;
            return true;
        }

        protected Task<Tuple<IEnumerable<string>, IFileWR>> FindManifestCsvRecordsAll(IFileWR manFileWR) => Task.Run(() => {
                var csvRecords = new List<string>();
                manFileWR.ReadStream(x => {
                    x.Position = 0;
                    using(var reader = new StreamReader(x)){
                        while(!reader.EndOfStream){
                            csvRecords.Add(reader.ReadLine());
                        }
                    }
                });
                return new Tuple<IEnumerable<string>,IFileWR>(csvRecords, manFileWR);
            });


        protected void ManifestRewriteRemoveKey(string key, IFileWR manFileWR){
            var resultText = System.Text.RegularExpressions.Regex.Replace(manFileWR.Read(), $"^{key},([0-9]+),([0-9]+),.*\n", string.Empty, System.Text.RegularExpressions.RegexOptions.Multiline);
            manFileWR.Clear();
            manFileWR.Write(resultText);
        }

        protected void ManifestRewriteUpdateKey(string key, ManifestItem newManItem, IFileWR manFileWR){
            var resultText = System.Text.RegularExpressions.Regex.Replace(manFileWR.Read(), $"^{key},([0-9]+),([0-9]+),.*\n", newManItem.ToCsv(), System.Text.RegularExpressions.RegexOptions.Multiline);
            manFileWR.Clear();
            manFileWR.Write(resultText);
        }
    }
}