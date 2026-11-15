namespace PgOutput2Json.MongoDb
{
    internal class ConfigKey
    {
        /// <summary>
        /// The position of the last saved change: "w/n", one value, so that it is saved at once.
        /// </summary>
        public const string Position = "Position";

        // before version 2.1 the two parts of the position had keys of their own, and were not saved at once
        public const string WalEnd = "WalEnd";
        public const string MessageNo = "MessageNo";
        public const string Schema = "Schema";
    }
}
