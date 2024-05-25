using System;
using System.Collections.Generic;
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
            return _indexFileWR.OpenStreamForWrite(fstream => {
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
                            fstream.Seek(currentPosition + i, SeekOrigin.Begin);
                            fstream.Write(new byte[indexItemFullBytes], 0, indexItemFullBytes);
                            ((MOFile.IStack)_delIndexFileWR).Push(BitConverter.GetBytes(currentPosition + i));
                            return new IndexItemToDelete(currentPosition + i > -1, _indexName, currentPosition + i , BitConverter.ToInt64(buffer, i + _numberOfKeyBytes), BitConverter.ToInt32(buffer, i + _numberOfKeyBytes + _numberOfPositionBytes));
                        }
                    }
                    currentPosition += buffer.Length;
                }
                return default;
            });
        }

        public int CountDeleted()
        {
            using var fstream = _delIndexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return 0;
            int indexItemFullBytes = TotalIndexItemBytes;
            return (int)(length / _numberOfPositionBytes);
        }

        public IEnumerable<ReadObject> Filter(Func<byte[], int, int, bool> predicate){
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                yield break;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(predicate(buffer, i, _numberOfKeyBytes)){
                        yield return new ReadObject(BitConverter.ToInt64(buffer, i + _numberOfKeyBytes), BitConverter.ToInt32(buffer, i + _numberOfKeyBytes + _numberOfPositionBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
        }

        public int Count(Func<byte[], int, int, bool> predicate){
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return 0;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            int counter = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(predicate(buffer, i, _numberOfKeyBytes)){
                        counter +=1;
                    }
                }
                currentPosition += buffer.Length;
            }
            return counter;
        }

        public bool Any(Func<byte[], int, int, bool> predicate){
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return false;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(predicate(buffer, i, _numberOfKeyBytes)){
                        return true;
                    }
                }
                currentPosition += buffer.Length;
            }
            return false;
        }
    }
}