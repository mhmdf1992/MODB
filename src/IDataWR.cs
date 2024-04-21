using System.Collections.Generic;
using System.IO;
using System.Text;
using MO.MOFile;

namespace MO.MODB{
    public interface IDataWR{
        IFileWR FlatFileWR {get;}
        long Size {get;}
        Encoding Encoding{get;}
        long Add(string value);
        long Add(Stream stream);
        long Erase(long position, int length);
        string Get(long position, int length);
        Stream GetStream(long position, int length);
    }
}