using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace MO.MODB{
    public class KeyValueIndexWR : KeyIndexWRBase, IIndexWR, IKeyValueIndexWR
    {
        public KeyValueIndexWR(string indexName, int numberOfKeyBytes, int numberOfPositionBytes, int numberOfLengthBytes, string path) : base(indexName, numberOfKeyBytes, numberOfPositionBytes, numberOfLengthBytes, path)
        {
        }

        public bool Any()
        {
            using var fstream = GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return false;
            var indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[_numberOfKeyBytes];
            var pattern = new byte[_numberOfKeyBytes];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(!CompareBytes(buffer, pattern, i)){
                        return true;
                    }
                }
                currentPosition += buffer.Length;
            }
            return false;
        }

        public int Count()
        {
            using var fstream = GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return 0;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            var pattern = new byte[_numberOfKeyBytes];
            long currentPosition = 0;
            var counter = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(!CompareBytes(buffer, pattern, i)){
                        counter ++;
                    }
                }
                currentPosition += buffer.Length;
            }
            return counter;
        }

        public bool Exists(byte[] pattern)
        {
            using var fstream = GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return false;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(CompareBytes(buffer, pattern, i)){
                        return true;
                    }
                }
                currentPosition += buffer.Length;
            }
            return false;
        }

        public IndexItemToRead Find(byte[] pattern)
        {
            using var fstream = GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return default;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(CompareBytes(buffer, pattern, i)){
                        var positionBytes = new byte[_numberOfPositionBytes];
                        var lengthBytes = new byte[_numberOfLengthBytes];
                        Buffer.BlockCopy(buffer, i + pattern.Length, positionBytes, 0, positionBytes.Length);
                        Buffer.BlockCopy(buffer, i + pattern.Length + positionBytes.Length, lengthBytes, 0, lengthBytes.Length);
                        return new IndexItemToRead(_indexName, currentPosition + (i *indexItemFullBytes), BitConverter.ToInt64(positionBytes), BitConverter.ToInt32(lengthBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
            return default;
        }

        public IndexItemToDelete Delete(byte[] pattern)
        {
            using var fstream = GetStreamForRead();
            long length = fstream.Length;
            if(length == 0)
                return default;
            int indexItemFullBytes = TotalIndexItemBytes;
            var buffer = new byte[indexItemFullBytes * 1000];
            long currentPosition = 0;
            while(currentPosition < length){
                var read = fstream.Read(buffer, 0, buffer.Length);
                for(int i = 0; i < read; i += indexItemFullBytes){
                    if(CompareBytes(buffer, pattern, i)){
                        var positionBytes = new byte[_numberOfPositionBytes];
                        var lengthBytes = new byte[_numberOfLengthBytes];
                        Buffer.BlockCopy(buffer, i + pattern.Length, positionBytes, 0, positionBytes.Length);
                        Buffer.BlockCopy(buffer, i + pattern.Length + positionBytes.Length, lengthBytes, 0, lengthBytes.Length);
                        var delPos = WriteBytes(new byte[indexItemFullBytes], currentPosition + i);
                        return new IndexItemToDelete(delPos > -1, _indexName, currentPosition + (i *indexItemFullBytes), BitConverter.ToInt64(positionBytes), BitConverter.ToInt32(lengthBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
            return default;
        }

        public IEnumerable<IndexItemToRead> All()
        {
            using var fstream = GetStreamForRead();
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
                    if(!CompareBytes(buffer, pattern, i)){
                        var positionBytes = new byte[_numberOfPositionBytes];
                        var lengthBytes = new byte[_numberOfLengthBytes];
                        Buffer.BlockCopy(buffer, i + pattern.Length, positionBytes, 0, positionBytes.Length);
                        Buffer.BlockCopy(buffer, i + pattern.Length + positionBytes.Length, lengthBytes, 0, lengthBytes.Length);
                        yield return new IndexItemToRead(_indexName, currentPosition + (i *indexItemFullBytes), BitConverter.ToInt64(positionBytes), BitConverter.ToInt32(lengthBytes));
                    }
                }
                currentPosition += buffer.Length;
            }
        }
    }
}