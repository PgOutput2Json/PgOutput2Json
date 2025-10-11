using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PgOutput2Json.Sqlite
{
    [JsonSerializable(typeof(List<ColumnInfo>))]
    internal partial class JsonContext : JsonSerializerContext
    {
    }
}
