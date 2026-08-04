using System.Text.Json;

namespace PgOutput2Json
{
    public static class MessageExtensions
    {
        public static bool TryGetWalEnd(this string? json, out ulong walEnd)
        {
            return json.TryGetWalSeq(out walEnd, out _);
        }

        /// <summary>
        /// Reads the deduplication key - the transaction final LSN ("w") and the
        /// message number within that transaction ("n") - from a published message.
        /// </summary>
        public static bool TryGetWalSeq(this string? json, out ulong walEnd, out ulong messageNo)
        {
            walEnd = 0;
            messageNo = 0;

            if (json == null) return false;

            using JsonDocument doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("n", out JsonElement messageNoProp)
                && messageNoProp.ValueKind == JsonValueKind.Number
                && messageNoProp.TryGetUInt64(out var messageNoValue))
            {
                messageNo = messageNoValue;
            }

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
