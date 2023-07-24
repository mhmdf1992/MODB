using MODB.ConcurrentFile;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MODB.FlatFileDB{
    public class ManifestFile : ConcurrentFileWR, IFileWR, IManifestCSVFile
    {
        protected int _key; public int Key => _key;

        public IEnumerable<IEnumerable<string>> GetTags(string text = null){
            System.Console.WriteLine(_key);
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    yield return csv.GetString(4).Split(' ').Where(x => string.IsNullOrEmpty(text) || x.Contains(text));
                }
            }
        }

        public ManifestItemMin? Find(string key, System.Threading.CancellationTokenSource cs){
            System.Console.WriteLine(_key);
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read() && ! cs.IsCancellationRequested)
                {
                    if(csv.GetString(0) == key){
                        cs.Cancel();
                        return new ManifestItemMin(csv.GetInt64(1), csv.GetInt32(2), _key);
                    }
                }
                return default(ManifestItemMin?);
            }
        }

        public IEnumerable<ManifestItemMin> FilterMin(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    if(((tags == null || !tags.Any()) || tags.Any(t => csv.GetString(4).Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItemMin(csv.GetInt64(1), csv.GetInt32(2), _key);
                }
            }
        }

        public IEnumerable<ManifestItem> Filter(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    var tagsString = csv.GetString(4);
                    if(((tags == null || !tags.Any()) || tags.Any(t => tagsString.Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItem(csv.GetString(0), csv.GetInt64(1), csv.GetInt32(2), timeStamp, _key, tagsString);
                }
            }
        }

        public IEnumerable<string> GetKeys(){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    yield return csv.GetString(0);
                }
            }
        }

        public IEnumerable<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    if(((tags == null || !tags.Any()) || tags.Any(t => csv.GetString(4).Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return csv.GetString(0);
                }
            }
        }

        public IEnumerable<ManifestItem> GetKeysOrdered(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    var tagsString = csv.GetString(4);
                    if(((tags == null || !tags.Any()) || tags.Any(t => tagsString.Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItem(csv.GetString(0), csv.GetInt64(1), csv.GetInt32(2), timeStamp, _key, tagsString);
                }
            }
        }

        public void Remove(string key){
            var resultText = System.Text.RegularExpressions.Regex.Replace(Read(), $"^{key},([0-9]+),([0-9]+),.*\n", string.Empty, System.Text.RegularExpressions.RegexOptions.Multiline);
            Clear();
            Write(resultText);
        }

        public void Update(string key, ManifestItem newManItem){
            var resultText = System.Text.RegularExpressions.Regex.Replace(Read(), $"^{key},([0-9]+),([0-9]+),.*\n", newManItem.ToCsv(), System.Text.RegularExpressions.RegexOptions.Multiline);
            Clear();
            Write(resultText);
        }

        public ManifestFile(string path, int key) : base(path)
        {
            _key = key;
            if(!File.Exists(_path))
                using(var fs = File.Create(_path)){
                }
        }
    }

    public interface IManifestCSVFile{
        IEnumerable<IEnumerable<string>> GetTags(string text = null);
        ManifestItemMin? Find(string key, System.Threading.CancellationTokenSource cs);
        IEnumerable<ManifestItemMin> FilterMin(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<ManifestItem> Filter(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<string> GetKeys();
        IEnumerable<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<ManifestItem> GetKeysOrdered(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        void Remove(string key);
        void Update(string key, ManifestItem newManItem);
    }
}