using System.Text.Json.Serialization;
namespace MODB.Client{
    public class DBInformation{
        [JsonPropertyName("name")]
        public string Name {get; set;}
        [JsonPropertyName("size")]
        public long Size {get; set;}
        [JsonPropertyName("manifests")]
        public int Manifests {get; set;}
    }
}