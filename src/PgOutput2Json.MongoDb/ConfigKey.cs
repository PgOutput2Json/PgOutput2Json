namespace PgOutput2Json.MongoDb
{
    internal class ConfigKey
    {
        /// <summary>
        /// The key of the position of the last saved change of a subscription: "w/n", one value, so that it is saved at once.
        /// </summary>
        public static string PositionOf(string subscriptionName) => $"Position:{subscriptionName}";

        // before version 2.2 the store had one position, under this key
        public const string Position = "Position";

        // before version 2.1 the two parts of the position had keys of their own, and were not saved at once
        public const string WalEnd = "WalEnd";
        public const string MessageNo = "MessageNo";
        public const string Schema = "Schema";
    }
}
