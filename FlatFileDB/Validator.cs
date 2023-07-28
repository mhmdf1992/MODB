using System;
using System.Linq;

namespace MODB.FlatFileDB{
    public class Validator{
        static readonly char[] KEY_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-@".ToArray();
        public static bool ValidateKey(string key) => string.IsNullOrEmpty(key) || key.Any(x => !KEY_ALLOWED_CHARS.Contains(x)) ? throw new ArgumentException($"{key} is not a valid key. keys must match ^[a-zA-Z0-9_.-@]+$", nameof(key)) : true;
    }
}