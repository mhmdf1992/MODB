namespace MO.MODB.Exceptions{
    public class IndexNotFoundException : System.ArgumentException{
        string _name;
        public IndexNotFoundException(string name): base(message: $"Index {name} does not exist", paramName: "Index"){
            _name = name;
        }
    }
}