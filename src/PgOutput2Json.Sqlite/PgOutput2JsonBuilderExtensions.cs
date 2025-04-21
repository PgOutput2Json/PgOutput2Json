using Microsoft.Data.Sqlite;
using System;

namespace PgOutput2Json.Sqlite
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseSqlite(this PgOutput2JsonBuilder builder,
                                                     Action<SqlitePublisherOptions>? configureAction = null)
        {
            var options = new SqlitePublisherOptions 
            { 
                ConnectionStringBuilder = new SqliteConnectionStringBuilder() 
            };

            options.ConnectionStringBuilder.DataSource = "pgoutput2json.s3db";
            options.ConnectionStringBuilder.Mode = SqliteOpenMode.ReadWriteCreate;

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new SqlitePublisherFactory(options));

            return builder;
        }
    }
}
