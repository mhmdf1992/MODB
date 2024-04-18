namespace MO.MODB.Exceptions{
    public class KeyNotFoundException : System.Exception{
        protected string _key;
        public string Key => _key;
        public KeyNotFoundException(string key): base($"Key {key} does not exist"){
            _key = key;
        }
    }

    public class UniqueKeyConstraintException : System.Exception{
        protected string _key;
        public string Key => _key;
        public UniqueKeyConstraintException(string key): base($"Key {key} already exist"){
            _key = key;
        }
    }
}