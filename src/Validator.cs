using System;
using System.Collections.Generic;
using System.Linq;

namespace MO.MODB{
    public class Validator{
        static readonly char[] INDEX_KEY_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-@".ToArray();
        public static bool ValidateKey(string key, int maxLength) => string.IsNullOrEmpty(key) || key.Length > maxLength ? throw new ArgumentException($"{key} is not a valid key. keys must be {maxLength} characters maximum length.", nameof(key)) : true;
        public static bool ValidateIndex(KeyValuePair<string, string> index, int maxLength) => 
            string.IsNullOrEmpty(index.Key) || index.Key.Length > maxLength || index.Key.Any(x => !INDEX_KEY_ALLOWED_CHARS.Contains(x)) 
             ? throw new ArgumentException($"{index.Key} is not a valid index key. index keys must match ^[a-zA-Z0-9_.-@]+$ {maxLength} characters maximum length.", nameof(index.Key))
              : !string.IsNullOrEmpty(index.Value) && index.Value.Length > maxLength 
               ? throw new ArgumentException($"{index.Value} is not a valid index value. index value must be {maxLength} characters maximum length.", nameof(index.Value))
                : true;
            
    }
}