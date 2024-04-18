using System;
using System.IO;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public abstract class KeyIndexWRBase : FileWR, IFileWR{
        protected string _indexName;
        protected int _numberOfKeyBytes;
        protected int _numberOfPositionBytes;
        protected int _numberOfLengthBytes;
        protected int TotalIndexItemBytes => _numberOfKeyBytes + _numberOfPositionBytes + _numberOfLengthBytes;
        public KeyIndexWRBase(string indexName, int numberOfKeyBytes, int numberOfPositionBytes, int numberOfLengthBytes, string path) : base(path){
            _indexName = indexName;
            _numberOfKeyBytes = numberOfKeyBytes;
            _numberOfPositionBytes = numberOfPositionBytes;
            _numberOfLengthBytes = numberOfLengthBytes;
            if(!File.Exists(_path)){
                using var fs = File.Create(_path);
                return;
            }
        }
        protected Func<byte[],byte[],int,bool> CompareBytes => (haystak, needle, offset) => {
            for(int i = 0; i < needle.Length; i ++){
                if(needle[i] != haystak[offset + i])
                    return false;
            }
            return true;
        };

        public void Add(byte[] bytes)
        {
            AppendBytes(bytes);
        }
    }
}