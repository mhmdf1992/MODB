using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace MODB.ConcurrentFile{
    public class ConcurrentFileWR : IFileWR
    {
        protected string _path;
        public FileInfo FileInfo => new FileInfo(_path);
        public long Size => !FileInfo.Exists ? 0 : FileInfo.Length;
        public ConcurrentFileWR(string path){
            _path = path;
            if(!Directory.Exists(Path.GetDirectoryName(path)))
                Directory.CreateDirectory(Path.GetDirectoryName(path));
        }
        public void Clear()
        {
            while(IsFileLockedForWrite()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)){
                    stream.SetLength(0);
                }
            }catch{
                throw;
            }
        }

        public bool IsFileLockedForRead()
        {
            try
            {
                using(FileStream stream = File.Open(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                }
            }
            catch (IOException)
            {
                return true;
            }
            return false;
        }

        public bool IsFileLockedForWrite()
        {
            try
            {
                using(FileStream stream = File.Open(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                {
                }
            }
            catch (IOException)
            {
                return true;
            }
            return false;
        }

        public string Read(long startPosition = 0, int? length = null)
        {
            while(IsFileLockedForRead()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite)){
                    if(stream.Length < startPosition + (length ?? 0))
                        throw new Exception("File length is less than requested length");
                    if(stream.Length == 0)
                        return string.Empty;
                    stream.Seek(startPosition, SeekOrigin.Begin);
                    byte[] b = new byte[length ?? (stream.Length - startPosition)];
                    stream.Read(b, 0, length ?? (int)(stream.Length - startPosition));
                    return System.Text.Encoding.UTF8.GetString(b);
                }
            }catch{
                throw;
            }
        }

        public void ReadStream(Action<Stream> action, long startPosition = 0, int? length = null)
        {
            while(IsFileLockedForRead()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite)){
                    if(stream.Length < startPosition + (length ?? 0))
                        throw new Exception("File length is less than requested length");
                    if(stream.Length == 0){
                        action(stream);
                        return;
                    }
                    stream.Seek(startPosition, SeekOrigin.Begin);
                    using(var resStream = new MemoryStream()){
                        CopyStream(stream, resStream, length ?? (int)(stream.Length - startPosition));
                        action(resStream);
                    }
                }
            }catch{
                throw;
            }
        }

        public void ReadStream(Action<Stream> action)
        {
            while(IsFileLockedForRead()){
                Thread.Sleep(10);
            }
            try{
                using (var stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite)){
                    action(stream);
                }
            }catch{
                throw;
            }
        }

        public long Write(string text, long startPosition = 0)
        {
            if(string.IsNullOrEmpty(text))
                return -1;
            while(IsFileLockedForWrite()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)){
                    if(stream.Length < (int) startPosition)
                        throw new Exception("File length is less than requested start position");
                    stream.Seek(startPosition, SeekOrigin.Begin);
                    stream.Write(System.Text.Encoding.UTF8.GetBytes(text), 0, text.Length);
                    return startPosition;
                }
            }catch{
                throw;
            }
        }

        public long WriteAtEnd(string text)
        {
            if(string.IsNullOrEmpty(text))
                return -1;
            while(IsFileLockedForWrite()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)){
                    var position = stream.Length;
                    stream.Seek(0, SeekOrigin.End);
                    stream.Write(System.Text.Encoding.UTF8.GetBytes(text), 0, text.Length);
                    return position;
                }
            }catch{
                throw;
            }
        }

        public long WriteAtEndStream(Stream stream)
        {
            if(stream == null || stream.Length == 0)
                return -1;
            while(IsFileLockedForWrite()){
                Thread.Sleep(10);
            }
            try{
                using (Stream fstream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)){
                    var position = fstream.Length;
                    fstream.Seek(0, SeekOrigin.End);
                    stream.Seek(0, SeekOrigin.Begin);
                    stream.CopyTo(fstream);
                    return position;
                }
            }catch{
                throw;
            }
        }

        public long WriteStream(Stream stream, long startPosition = 0)
        {
            if(stream == null || stream.Length == 0)
                return -1;
            while(IsFileLockedForWrite()){
                Thread.Sleep(10);
            }
            try{
                using (Stream fstream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)){
                    if(fstream.Length < (int) startPosition)
                        throw new Exception("File length is less than requested start position");
                    fstream.Seek(startPosition, SeekOrigin.Begin);
                    stream.Seek(0, SeekOrigin.Begin);
                    stream.CopyTo(fstream);
                    return startPosition;
                }
            }catch{
                throw;
            }
        }

        protected void CopyStream(Stream input, Stream output, int bytes){
            byte[] buffer = new byte[32768];
            int read;
            while (bytes > 0 && 
                (read = input.Read(buffer, 0, Math.Min(buffer.Length, bytes))) > 0){
                output.Write(buffer, 0, read);
                bytes -= read;
            }
        }

        public IEnumerable<string> Read(IEnumerable<ReadObj> readObjs)
        {
            List<string> result = new List<string>();
            while(IsFileLockedForRead()){
                Thread.Sleep(10);
            }
            try{
                using (Stream stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite)){
                    if(stream.Length == 0)
                        return result;
                    foreach(var obj in readObjs){
                        if(stream.Length < obj.Position + (obj.Length ?? 0))
                            throw new Exception("File length is less than requested length");
                        stream.Seek(obj.Position, SeekOrigin.Begin);
                        byte[] b = new byte[obj.Length ?? (stream.Length - obj.Position)];
                        stream.Read(b, 0, obj.Length ?? (int)(stream.Length - obj.Position));
                        result.Add(System.Text.Encoding.UTF8.GetString(b));
                    }
                    return result;
                }
            }catch{
                throw;
            }
        }

        public T ReadStream<T>(Func<Stream, T> func)
        {
            while(IsFileLockedForRead()){
                Thread.Sleep(10);
            }
            try{
                using (var stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite)){
                    stream.Seek(0, SeekOrigin.Begin);
                    return func(stream);
                }
            }catch{
                throw;
            }
        }
    }
}