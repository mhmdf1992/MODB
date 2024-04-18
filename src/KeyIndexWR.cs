namespace MO.MODB{
    public class KeyIndexWR : KeyIndexWRBase, IIndexWR
    {
        public KeyIndexWR(string indexName, int numberOfKeyBytes, int numberOfPositionBytes, int numberOfLengthBytes, string path) : base(indexName, numberOfKeyBytes, numberOfPositionBytes, numberOfLengthBytes, path)
        {
        }
    }
}