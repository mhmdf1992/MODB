namespace MO.MODB.Exceptions{
    public class InvalidIndexException : System.Exception{
        public InvalidIndexException(): base($"Invalid Index"){
        }
    }
}