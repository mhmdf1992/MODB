using System;
using System.Collections.Generic;
using System.Text;

namespace MO.MODB{
    public static class Converter{
        public readonly static Dictionary<string, Func<object, byte[]>> ToBytes = new Dictionary<string, Func<object, byte[]>>(){
            {typeof(string).Name, obj => Encoding.UTF8.GetBytes((string)obj)},
            {typeof(short).Name, obj => BitConverter.GetBytes(Convert.ToInt16(obj))},
            {typeof(int).Name, obj => BitConverter.GetBytes(Convert.ToInt32(obj))},
            {typeof(long).Name, obj => BitConverter.GetBytes(Convert.ToInt64(obj))},
            {typeof(double).Name, obj => BitConverter.GetBytes(Convert.ToDouble(obj))},
            {typeof(bool).Name, obj => BitConverter.GetBytes(Convert.ToBoolean(obj))}
        };

        public readonly static Dictionary<string, Func<byte[], int, dynamic>> To = new Dictionary<string, Func<byte[], int, dynamic>>(){
            {typeof(string).Name, (bytes, offset) => Encoding.UTF8.GetString(bytes)},
            {typeof(short).Name, (bytes, offset) => BitConverter.ToInt16(bytes, offset)},
            {typeof(int).Name, (bytes, offset) => BitConverter.ToInt32(bytes, offset)},
            {typeof(long).Name, (bytes, offset) => BitConverter.ToInt64(bytes, offset)},
            {typeof(double).Name, (bytes, offset) => BitConverter.ToDouble(bytes, offset)},
            {typeof(bool).Name, (bytes, offset) => BitConverter.ToBoolean(bytes, offset)}
        };

        public readonly static Dictionary<string, int> NUMBER_OF_BYTES = new Dictionary<string, int>(){
            {typeof(short).Name, 2},
            {typeof(int).Name, 4},
            {typeof(long).Name, 8},
            {typeof(double).Name, 8},
            {typeof(bool).Name, 1}
        };
    }
}