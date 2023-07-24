using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MODB.ConcurrentFile;

namespace MODB.FlatFileDB{
    public struct DBConfig{
        int _numberOfManifests; public int NumberOfManifests => _numberOfManifests;
        public DBConfig(int numberOfManifests){
            _numberOfManifests = numberOfManifests;
        }
        public string ToConfigString() => $"numberOfManifests={_numberOfManifests}";
        public static DBConfig Parse(string configText){
            var config = configText.Split(Environment.NewLine).Select(x => {
                var keyValuePair = x.Split('=');
                return new KeyValuePair<string, string>(keyValuePair[0], keyValuePair[1]);
            }).ToDictionary(x => x.Key, x => x.Value);
            return new DBConfig(Helper.ConvertToInt(config["numberOfManifests"].ToArray()));
        }
    }
}