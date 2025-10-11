using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PgOutput2Json.DynamoDb
{
    [JsonSerializable(typeof(List<ColumnInfo>))]
    internal partial class JsonContext : JsonSerializerContext
    {
    }
}
