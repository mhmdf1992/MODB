using MODB.ConcurrentFile;
using Sylvan;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MODB.FlatFileDB{
    public class ManifestFile : ConcurrentFileWR, IFileWR, IManifestCSVFile
    {
        protected int _key; public int Key => _key;
        protected StringPool _stringPool;

        public IEnumerable<IEnumerable<string>> GetTags(string text = null){

            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    if(csv.GetInt16(5) == 0)
                        yield return csv.GetString(4).Split(' ').Where(x => string.IsNullOrEmpty(text) || x.Contains(text));
                }
            }
        }

        public ManifestItemMin? Find(string key, System.Threading.CancellationTokenSource cs){

            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read() && ! cs.IsCancellationRequested)
                {
                    if(csv.GetInt16(5) == 0 && csv.GetString(0) == key){
                        cs.Cancel();
                        return new ManifestItemMin(csv.GetInt64(1), csv.GetInt32(2), _key);
                    }
                }
            }
            return default;
        }

        public ManifestItemMinToDel? FindToDelete(string key, System.Threading.CancellationTokenSource cs){

            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                long offset = 0;
                while(csv.Read() && ! cs.IsCancellationRequested)
                {
                    offset += csv.GetRawRecordSpan().Length;
                    if(csv.GetInt16(5) == 0 && csv.GetString(0) == key){
                        cs.Cancel();
                        return new ManifestItemMinToDel(csv.GetInt64(1), csv.GetInt32(2), _key, offset - Environment.NewLine.Length);
                    }
                }
                return default;
            }
        }

        public IEnumerable<ManifestItemMin> FilterMin(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    if(csv.GetInt16(5) == 0 && ((tags == null || !tags.Any()) || tags.Any(t => csv.GetString(4).Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItemMin(csv.GetInt64(1), csv.GetInt32(2), _key);
                }
            }
        }

        public IEnumerable<ManifestItem> Filter(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    var tagsString = csv.GetString(4);
                    if(csv.GetInt16(5) == 0 && ((tags == null || !tags.Any()) || tags.Any(t => tagsString.Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItem(csv.GetString(0), csv.GetInt64(1), csv.GetInt32(2), timeStamp, _key, tagsString);
                }
            }
        }

        public IEnumerable<string> GetKeys(){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    if(csv.GetInt16(5) == 0)
                        yield return csv.GetString(0);
                }
            }
        }

        public IEnumerable<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    if(csv.GetInt16(5) == 0 && ((tags == null || !tags.Any()) || tags.Any(t => csv.GetString(4).Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return csv.GetString(0);
                }
            }
        }

        public IEnumerable<ManifestItem> GetKeysOrdered(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null){    
            using(var csv = Sylvan.Data.Csv.CsvDataReader.Create(_path, new Sylvan.Data.Csv.CsvDataReaderOptions(){ HasHeaders = false, StringFactory = _stringPool.GetString})){
                while(csv.Read())
                {
                    var timeStamp = csv.GetInt64(3);
                    var tagsString = csv.GetString(4);
                    if(csv.GetInt16(5) == 0 && ((tags == null || !tags.Any()) || tags.Any(t => tagsString.Contains(t))) && (timeStampFrom == null || timeStamp >= timeStampFrom) && (timeStampTo == null || timeStamp <= timeStampTo))
                        yield return new ManifestItem(csv.GetString(0), csv.GetInt64(1), csv.GetInt32(2), timeStamp, _key, tagsString);
                }
            }
        }

        public void Remove(long delPosition){
            Write("1", delPosition);
        }

        public ManifestFile(string path, int key, StringPool stringPool) : base(path)
        {
            _key = key;
            _stringPool = stringPool;
            if(!File.Exists(_path))
                using(var fs = File.Create(_path)){
                }
        }
    }

    public interface IManifestCSVFile{
        IEnumerable<IEnumerable<string>> GetTags(string text = null);
        ManifestItemMin? Find(string key, System.Threading.CancellationTokenSource cs);
        ManifestItemMinToDel? FindToDelete(string key, System.Threading.CancellationTokenSource cs);
        IEnumerable<ManifestItemMin> FilterMin(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<ManifestItem> Filter(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<string> GetKeys();
        IEnumerable<string> GetKeys(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        IEnumerable<ManifestItem> GetKeysOrdered(IEnumerable<string> tags = null, long? timeStampFrom = null, long? timeStampTo = null);
        void Remove(long delPosition);
    }
}