namespace MO.MODB.Exceptions{
    public class IndexNotFoundException : System.Exception{
        string _name;
        public IndexNotFoundException(string name): base($"Index {name} does not exist"){
            _name = name;
        }
    }
}