using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using MO.MOFile;

namespace MO.MODB{
    public class DataWR : IDataWR
    {
        protected string _flatFilePath;
        protected string _delFlatFilePath;
        protected IFileWR _flatFileWR;
        protected IFileWR _delFlatFileWR;
        public IFileWR FlatFileWR => _flatFileWR;
        public long Size => _flatFileWR.Size + _delFlatFileWR.Size;
        public Encoding Encoding => _flatFileWR.Encoding;

        public DataWR(string path){
            _flatFilePath = path;
            _delFlatFilePath = $"{path}.del";
            _flatFileWR = new FileWR(_flatFilePath);
            _delFlatFileWR = new FileWR(_delFlatFilePath);
            if(!File.Exists(_flatFilePath)){
                using var fs = File.Create(_flatFilePath);
            }
            if(!File.Exists(_delFlatFilePath)){
                using var fs = File.Create(_delFlatFilePath);
            }
        }

        protected long GetPositionToWrite(byte[] bytes){
            return _delFlatFileWR.OpenStreamForWrite(fstream => {
                long length = fstream.Length;
                if(length == 0)
                    return -1;
                int indexItemFullBytes = 12;
                var buffer = new byte[indexItemFullBytes * 1000];
                long currentPosition = 0;
                int count = (int)(fstream.Length / indexItemFullBytes);
                int countEmpty = 0;
                Tuple<int, long, long> res = new Tuple<int, long, long>(int.MaxValue, -1, -1); 
                while(currentPosition < length){
                    var read = fstream.Read(buffer, 0, buffer.Length);
                    for(int i = 0; i < read; i += indexItemFullBytes){
                        if(buffer.CompareBytes(new byte[4], i + 8)){
                            countEmpty += 1;
                            continue;
                        }
                        var delLength = BitConverter.ToInt32(buffer, i + 8);
                        var delPosition = BitConverter.ToInt64(buffer, i);
                        var diff = delLength - bytes.Length;
                        if(diff == 0){
                            res = new Tuple<int, long, long>(diff, delPosition, currentPosition + i);
                            goto CheckResult;
                        }
                        res = diff >= 0 && diff < res.Item1 ? new Tuple<int, long, long>(diff, delPosition, currentPosition + i) : res;
                    }
                    currentPosition += buffer.Length;
                }
                CheckResult:
                if(res.Item3 > -1){
                    fstream.Seek(res.Item3, SeekOrigin.Begin);
                    fstream.Write(new byte[12], 0, 12);
                    countEmpty += 1;
                }
                if(count == countEmpty)
                    fstream.SetLength(0);
                return res.Item2;
            });
        }

        public long Add(string value)
        {
            var bytes = _flatFileWR.Encoding.GetBytes(value);
            var position = GetPositionToWrite(bytes);
            if(position > -1)
                return _flatFileWR.WriteBytes(bytes, position);
            else
                return _flatFileWR.AppendBytes(bytes);
        }

        public long Add(Stream stream)
        {
            var bytes = new byte[stream.Length];
            stream.Read(bytes, 0, bytes.Length);
            var position = GetPositionToWrite(bytes);
            if(position > -1)
                return _flatFileWR.WriteBytes(bytes, position);
            else
                return _flatFileWR.AppendBytes(bytes);
        }

        public long Erase(long position, int length)
        {
            var pos = _flatFileWR.WriteBytes(new byte[length], position);
            if(pos > -1){
                var posBytes = BitConverter.GetBytes(position);
                var lenBytes = BitConverter.GetBytes(length);
                var bytesToWrite = new byte[posBytes.Length + lenBytes.Length];
                Buffer.BlockCopy(posBytes, 0, bytesToWrite, 0, posBytes.Length);
                Buffer.BlockCopy(lenBytes, 0, bytesToWrite, posBytes.Length, lenBytes.Length);
                _delFlatFileWR.AppendBytes(bytesToWrite);
            }
            return pos;
        }

        public string Get(long position, int length) => _flatFileWR.Read(position, length);

        public Stream GetStream(long position, int length){
            using var stream = _flatFileWR.GetStreamForRead(position);
            var buffer = new byte[length];
            stream.Read(buffer, 0, length);
            return new MemoryStream(buffer);
        }
    }
}