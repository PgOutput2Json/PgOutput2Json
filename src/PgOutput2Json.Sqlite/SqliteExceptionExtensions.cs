using Microsoft.Data.Sqlite;

namespace PgOutput2Json.Sqlite
{
    internal static class SqliteExceptionExtensions
    {
        public static bool IsPrimaryKeyViolation(this SqliteException ex)
        {
            return ex.SqliteErrorCode == 19 && ex.SqliteExtendedErrorCode == 1555;
        }
    }
}
