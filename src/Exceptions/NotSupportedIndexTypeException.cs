namespace MO.MODB.Exceptions{
    public class NotSupportedIndexTypeException : System.Exception{
        protected string _type;
        public string Type => _type;
        protected string _indexName;
        public string IndexName => _indexName;
        public NotSupportedIndexTypeException(string type, string indexName): base($"{indexName} Type {type} is not supported."){
            _type = type;
        }
    }
}