namespace MO.MODB.Exceptions{
    public class InvalidIndexException : System.Exception{
        string _name;
        public InvalidIndexException(string name): base($"Invalid Index {name}"){
            _name = name;
        }
    }
}