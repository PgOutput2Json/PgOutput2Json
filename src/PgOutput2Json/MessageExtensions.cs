using System.Text.Json;

namespace PgOutput2Json
{
    public static class MessageExtensions
    {
        public static bool TryGetWalEnd(this string? json, out ulong walEnd)
        {
            walEnd = 0;
            if (json == null) return false;

            using JsonDocument doc = JsonDocument.Parse(json);

            if ((doc.RootElement.TryGetProperty("w", out JsonElement prop) || doc.RootElement.TryGetProperty("_we", out prop))
                && prop.ValueKind == JsonValueKind.Number
                && prop.TryGetUInt64(out var value))
            {
                walEnd = value;
                return true;
            }

            return false;
        }
    }
}
