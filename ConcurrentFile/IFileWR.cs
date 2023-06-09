using System.IO;
using System;

namespace MODB.ConcurrentFile{
    public interface IFileWR{
        void ReadStream(Action<Stream> action, long startPosition = 0, int? length = null);
        string Read(long startPosition = 0, int? length = null);
        long Write(string text, long startPosition = 0);
        long WriteAtEnd(string text);
        long WriteStream(Stream stream, long startPosition = 0);
        long WriteAtEndStream(Stream stream);
        bool IsFileLockedForWrite();
        bool IsFileLockedForRead();
        void Clear();
        FileInfo FileInfo {get;}
        long Size {get;}
    }
}