namespace PgOutput2Json.IntegrationTests.Outputs
{
    internal static class StoreMessage
    {
        /// <summary>
        /// A change of the main stream at a position, in the compact write mode the stores use.
        /// </summary>
        public static JsonMessage Create(ulong txFinalLsn, ulong messageNo)
        {
            var message = new JsonMessage { TxFinalLsn = txFinalLsn, MessageNo = messageNo };

            message.TableName.Append("public.os_position");
            message.Json.Append($"{{\"c\":\"I\",\"w\":{txFinalLsn},\"n\":{messageNo},\"s\":[\"public.os_position\",[\"id\",1,23]],\"r\":[1]}}");

            return message;
        }
    }
}
