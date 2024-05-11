using System;
using System.Collections.Generic;
using System.Linq;
using MO.MODB.Exceptions;

namespace MO.MODB{
    public class Validator{
        static readonly char[] INDEX_NAME_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@".ToArray();
        static readonly char[] DB_NAME_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@.".ToArray();
        static readonly int MAXIMUM_INDEX_VALUE_LENGTH = 256;
        static readonly int MAXIMUM_INDEX_NAME_LENGTH = 64;
        static readonly int MAXIMUM_DB_NAME_LENGTH = 64;
        static readonly Dictionary<string, CompareOperators[]> DATA_TYPE_VALID_COMPARE_OPERATORS = new Dictionary<string, CompareOperators[]>(){
            {typeof(string).Name, new CompareOperators[]{CompareOperators.Equal, CompareOperators.NotEqual, CompareOperators.Contain, CompareOperators.NotContain}},
            {typeof(short).Name, Enum.GetValues(typeof(CompareOperators)).Cast<CompareOperators>().Where(c => !c.Equals(CompareOperators.Contain) || !c.Equals(CompareOperators.NotContain)).ToArray()},
            {typeof(int).Name, Enum.GetValues(typeof(CompareOperators)).Cast<CompareOperators>().Where(c => !c.Equals(CompareOperators.Contain) || !c.Equals(CompareOperators.NotContain)).ToArray()},
            {typeof(long).Name, Enum.GetValues(typeof(CompareOperators)).Cast<CompareOperators>().Where(c => !c.Equals(CompareOperators.Contain) || !c.Equals(CompareOperators.NotContain)).ToArray()},
            {typeof(double).Name, Enum.GetValues(typeof(CompareOperators)).Cast<CompareOperators>().Where(c => !c.Equals(CompareOperators.Contain) || !c.Equals(CompareOperators.NotContain)).ToArray()},
            {typeof(bool).Name, Enum.GetValues(typeof(CompareOperators)).Cast<CompareOperators>().Where(c => !c.Equals(CompareOperators.Contain) || !c.Equals(CompareOperators.NotContain)).ToArray()}
        }; 
        

        public static bool ValidateIndexValue(byte[] bytesValue, string indexName, object value, string indexType) =>
            bytesValue == null || 
            bytesValue.Length > MAXIMUM_INDEX_VALUE_LENGTH
             ? throw new ArgumentException($"{value} is not valid for {indexType} {indexName}. {MAXIMUM_INDEX_VALUE_LENGTH} characters maximum length.", nameof(value))
             : true;
        public static bool ValidateIndexName(string name) => 
            string.IsNullOrEmpty(name) || 
            name.Length > MAXIMUM_INDEX_NAME_LENGTH ||
            name.Any(x => !INDEX_NAME_ALLOWED_CHARS.Contains(x))
             ? throw new ArgumentException($"{name} is not a valid index name. Index names must match ^[a-zA-Z0-9_-@]+$ {MAXIMUM_INDEX_NAME_LENGTH} characters maximum length.", nameof(name))
             : true;
        public static bool ValidateDBName(string name) => 
            string.IsNullOrEmpty(name) || 
            name.Length > MAXIMUM_DB_NAME_LENGTH ||
            name.Any(x => !DB_NAME_ALLOWED_CHARS.Contains(x))
             ? throw new ArgumentException($"{name} is not a valid database name. Database names must match ^[a-zA-Z0-9_-@]+$ {MAXIMUM_DB_NAME_LENGTH} characters maximum length.", nameof(name))
             : true;
        public static bool ValidateCompareOperatorWithDataType(CompareOperators compareOperator, string dataType) =>
            !DATA_TYPE_VALID_COMPARE_OPERATORS[dataType].Any(c => c.Equals(compareOperator)) ? throw new CompareOperatorIndexTypeMissMatchException(compareOperator, dataType) : true;
        public static bool ValidateType(string type, string indexName) => string.IsNullOrEmpty(type) || ! Converter.To.ContainsKey(type) ? throw new NotSupportedIndexTypeException(type, indexName) : true;
        public static bool ValidateKeyType(string type, string indexName) => string.IsNullOrEmpty(type) || type == typeof(bool).Name || ! Converter.To.ContainsKey(type) ? throw new NotSupportedIndexTypeException(type, indexName) : true;
    }
}