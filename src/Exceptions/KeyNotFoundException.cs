namespace MO.MODB.Exceptions{
    public class KeyNotFoundException : System.Exception{
        protected object _key;
        public object Key => _key;
        public KeyNotFoundException(object key): base($"Key {key} does not exist"){
            _key = key;
        }
    }

    public class DBNotFoundException : System.Exception{
        protected object _key;
        public object Key => _key;
        public DBNotFoundException(object key): base($"Database {key} does not exist"){
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