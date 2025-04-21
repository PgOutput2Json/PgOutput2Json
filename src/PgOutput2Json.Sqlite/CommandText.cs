namespace PgOutput2Json.Sqlite
{
    internal class CommandText
    {
        public const string ConfigInsert = "INSERT INTO __pg2j_config (cfg_key, cfg_value) VALUES (@cfg_key, @cfg_value)";

        public const string ConfigUpdate = "UPDATE __pg2j_config SET cfg_value = @cfg_value WHERE cfg_key = @cfg_key";

        public const string ConfigSelect = "SELECT cfg_value FROM __pg2j_config WHERE cfg_key = @cfg_key";

        public const string ConfigCreate = @"
CREATE TABLE IF NOT EXISTS __pg2j_config (
    cfg_key TEXT NOT NULL,
    cfg_value TEXT,
    CONSTRAINT __pg2j_config_pk PRIMARY KEY (cfg_key)
)
";
        public const string ConfigColKey = "cfg_key";
        public const string ConfigColValue = "cfg_value";
    }
}
