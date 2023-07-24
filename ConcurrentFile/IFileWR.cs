using System.IO;
using System;
using System.Collections.Generic;

namespace MODB.ConcurrentFile{
    public interface IFileWR{
        void ReadStream(Action<Stream> action, long startPosition = 0, int? length = null);
        void ReadStream(Action<Stream> action);
        T ReadStream<T>(Func<Stream, T> func);
        string Read(long startPosition = 0, int? length = null);
        IEnumerable<string> Read(IEnumerable<ReadObj> readObjs);
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