using System;
using System.Collections.Generic;
using MO.MOFile;

namespace MO.MODB{
    public class KeyValueIndexWR : KeyIndexWRBase, IIndexWR, IKeyValueIndexWR
    {

        public KeyValueIndexWR(string indexName, int numberOfKeyBytes, int numberOfPositionBytes, int numberOfLengthBytes, string path) : base(indexName, numberOfKeyBytes, numberOfPositionBytes, numberOfLengthBytes, path)
        {
        }

        public bool Any() => Count() > 0;

        public int Count()
        {
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return 0;
            int indexItemFullBytes = TotalIndexItemBytes;
            return (int)(length / indexItemFullBytes) - CountDeleted();
        }

        public bool Exists(byte[] key)
        {
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
                    if(buffer.CompareBytes(key, i)){
                        return true;
                    }
                }
                currentPosition += buffer.Length;
            }
            return false;
        }

        public IndexItemToRead Find(byte[] key)
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
                    if(buffer.CompareBytes(key, i)){
                        return new IndexItemToRead(_indexName, currentPosition + i, BitConverter.ToInt64(buffer, i + key.Length), BitConverter.ToInt32(buffer, i + key.Length + _numberOfPositionBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
            return default;
        }

        public IndexItemToDelete Delete(byte[] key)
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
                    if(buffer.CompareBytes(key, i)){
                        var delPos = _indexFileWR.WriteBytes(new byte[indexItemFullBytes], currentPosition + i);
                        ((MOFile.IStack)_delIndexFileWR).Push(BitConverter.GetBytes(currentPosition + i));
                        return new IndexItemToDelete(delPos > -1, _indexName, currentPosition + i, BitConverter.ToInt64(buffer, i + key.Length), BitConverter.ToInt32(buffer, i + key.Length + _numberOfPositionBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
            return default;
        }

        public IEnumerable<ReadObject> All()
        {
            using var fstream = _indexFileWR.GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                yield break;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            var pattern = new byte[_numberOfKeyBytes];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(!buffer.CompareBytes(pattern, i)){
                        yield return new ReadObject(BitConverter.ToInt64(buffer, i + pattern.Length), BitConverter.ToInt32(buffer, i + pattern.Length + _numberOfPositionBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
        }
    }
}