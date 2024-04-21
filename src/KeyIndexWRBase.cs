using System;
using System.IO;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public abstract class KeyIndexWRBase{
        protected string _indexFilePath;
        protected string _delIndexFilePath;
        protected IFileWR _indexFileWR;
        protected IFileWR _delIndexFileWR;
        protected string _indexName;
        protected int _numberOfKeyBytes;
        protected int _numberOfPositionBytes;
        protected int _numberOfLengthBytes;
        protected int TotalIndexItemBytes => _numberOfKeyBytes + _numberOfPositionBytes + _numberOfLengthBytes;
        public long Size => _indexFileWR.Size + _delIndexFileWR.Size;
        public KeyIndexWRBase(string indexName, int numberOfKeyBytes, int numberOfPositionBytes, int numberOfLengthBytes, string path){
            _indexName = indexName;
            _numberOfKeyBytes = numberOfKeyBytes;
            _numberOfPositionBytes = numberOfPositionBytes;
            _numberOfLengthBytes = numberOfLengthBytes;
            _indexFilePath = path;
            _delIndexFilePath = $"{path}.del";
            _indexFileWR = new FileWR(_indexFilePath);
            _delIndexFileWR = new FileWR(_delIndexFilePath);
            if(!File.Exists(_indexFilePath)){
                using var fs = File.Create(_indexFilePath);
            }
            if(!File.Exists(_delIndexFilePath)){
                using var fs = File.Create(_delIndexFilePath);
            }
        }

        public void Add(byte[] bytes)
        {
            var delIndexPosition = ((IStack)_delIndexFileWR).Pop(_numberOfPositionBytes);
            if(delIndexPosition == null){
                _indexFileWR.AppendBytes(bytes);
                return;
            }
            var position = BitConverter.ToInt64(delIndexPosition);
            _indexFileWR.WriteBytes(bytes, position);
                
        }

        public void AddRange(byte[][] range)
        {
            _indexFileWR.AppendBytes(range);
        }

        public void Clear()
        {
            _indexFileWR.Clear();
            _delIndexFileWR.Clear();
        }

        public IndexItemToDelete DeleteByPosition(byte[] position)
        {
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return default;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(buffer.CompareBytes(position, i + _numberOfKeyBytes)){
                        var positionBytes = new byte[_numberOfPositionBytes];
                        var lengthBytes = new byte[_numberOfLengthBytes];
                        Buffer.BlockCopy(buffer, i + _numberOfKeyBytes, positionBytes, 0, positionBytes.Length);
                        Buffer.BlockCopy(buffer, i + _numberOfKeyBytes + positionBytes.Length, lengthBytes, 0, lengthBytes.Length);
                        var delPos = _indexFileWR.WriteBytes(new byte[indexItemFullBytes], currentPosition + i);
                        ((MOFile.IStack)_delIndexFileWR).Push(BitConverter.GetBytes(currentPosition + i));
                        return new IndexItemToDelete(delPos > -1, _indexName, currentPosition + i , BitConverter.ToInt64(positionBytes), BitConverter.ToInt32(lengthBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
            return default;
        }
    }
}