using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MODB.Api.Json{
    public class MODBRecordJsonConverter : JsonConverter<string>
    {
        public override string Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options) =>
                reader.GetString();

        public override void Write(
            Utf8JsonWriter writer,
            string value,
            JsonSerializerOptions options) =>
                writer.WriteRawValue(value);
    }
}