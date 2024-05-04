namespace MO.MODB.Exceptions{
    public class KeyIndexTypeMissMatchException : System.Exception{
        string _keyType;
        string _indexType;
        public KeyIndexTypeMissMatchException(string keyType, string indexType): base($"Can not compare KeyType {keyType} and IndexType {indexType}"){
            _indexType = indexType;
            _keyType = keyType;
        }
    }
}