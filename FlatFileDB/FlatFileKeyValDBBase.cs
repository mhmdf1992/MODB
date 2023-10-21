using MODB.ConcurrentFile;
using Sylvan;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MODB.FlatFileDB{
    public class FlatFileKeyValDBBase{
        protected DBStatus _status;
        protected DBConfig _dbConfig;
        protected string _path;
        protected string _flatFilePath;
        protected IFileWR _flatFileWR;
        protected IFileWR _configFileWR;
        protected Dictionary<int, IManifestCSVFile> _manFileWRs;
        protected string _name;
        public string Name => _name;
        public long Size => _flatFileWR.Size + _manFileWRs.Values.Sum(x => (x as IFileWR).Size);
        public DBConfig Config => _dbConfig;
        public DBStatus Status => _status;
        public string LastClean => _flatFileWR.Size == 0 ? "" : _flatFileWR.FileInfo.CreationTimeUtc.ToString();
        protected StringPool _stringPool;
        public FlatFileKeyValDBBase(string path, int numberOfManifestFiles = 10, DBStatus status = DBStatus.READY){
            _path = path;
            _status = status;
            _stringPool = new StringPool();
            try{
                if(numberOfManifestFiles <= 0)
                    throw new ArgumentException(paramName: nameof(numberOfManifestFiles), message: "Value should be greater than zero");
                var name = Path.GetFileName(_path);
                _name = name;
                var _flatFilePath = Path.Combine(path, $"{name}.dat");
                _flatFileWR = new ConcurrentFileWR(_flatFilePath);
                _manFileWRs = new Dictionary<int, IManifestCSVFile>();
                LoadConfig(numberOfManifestFiles);
                LoadManifests(_dbConfig.NumberOfManifests);
            }catch{throw;}
        }

        protected void LoadManifests(int numberOfManFiles){
            var name = Path.GetFileName(_path);
            var files = Directory.GetFiles(_path, "*.man");
            if(files == null || !files.Any()){
                for(int i = 1; i <= numberOfManFiles; i ++){
                    _manFileWRs.Add(i, new ManifestFile(Path.Combine(_path, $"{name}({i}).man"), i, _stringPool));
                }
                return;
            }
            foreach(var file in files){
                var regResult = System.Text.RegularExpressions.Regex.Match(Path.GetFileName(file), @"(?<=\()\d+(?=\))");
                var number = Helper.ConvertToInt(regResult.Value.ToArray());
                _manFileWRs.Add(number, new ManifestFile(file, number, _stringPool));
            }
        }

        protected void LoadConfig(int numberOfManifestFiles){
            var name = Path.GetFileName(_path);
            var files = Directory.GetFiles(_path, "*.config");
            if(files == null || !files.Any()){
                _configFileWR = new ConcurrentFileWR(Path.Combine(_path, $"{name}.config"));
                _configFileWR.WriteAtEnd(new DBConfig(numberOfManifestFiles).ToConfigString());
                _dbConfig = new DBConfig(numberOfManifestFiles);
                return;
            }
            _configFileWR = new ConcurrentFileWR(files.First());
            _dbConfig = DBConfig.Parse(_configFileWR.Read());
        }

        protected KeyValuePair<int,IManifestCSVFile> GetManifestWR() {
            return _manFileWRs.OrderBy(x => (x.Value as IFileWR).Size).Select(x => x).FirstOrDefault();
        }

        protected bool ManifestContainsItem(string key, out ManifestItemMin? manifestItem){
            var cs = new System.Threading.CancellationTokenSource();
            return (manifestItem = Task.WhenAll(_manFileWRs.Values.Select(x => Task.Run(() => x.Find(key, cs)))).Result
                .FirstOrDefault(x => x != null)) != null;
        }

        protected bool ManifestContainsItemToDelete(string key, out ManifestItemMinToDel? manifestItem){
            var cs = new System.Threading.CancellationTokenSource();
            return (manifestItem = Task.WhenAll(_manFileWRs.Values.Select(x => Task.Run(() => x.FindToDelete(key, cs)))).Result
                .FirstOrDefault(x => x != null)) != null;
        }
    }
}