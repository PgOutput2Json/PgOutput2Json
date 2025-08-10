using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PgOutput2Json
{
    [JsonSerializable(typeof(List<string>))]
    internal partial class JsonContext : JsonSerializerContext
    {
    }
}
