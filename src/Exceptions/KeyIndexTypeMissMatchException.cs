namespace MO.MODB.Exceptions{
    public class KeyIndexTypeMissMatchException : System.ArgumentException{
        string _keyType;
        string _indexType;
        public KeyIndexTypeMissMatchException(object value, string keyType, string indexType): base(message: $"Can not compare {keyType} {value} to IndexType {indexType}", paramName: "value"){
            _indexType = indexType;
            _keyType = keyType;
        }
    }
}