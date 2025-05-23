﻿using Microsoft.Data.Sqlite;
using PgOutput2Json.Sqlite;
using System;

namespace PgOutput2Json
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseSqlite(this PgOutput2JsonBuilder builder,
                                                     Action<SqlitePublisherOptions>? configureAction = null)
        {
            // Sqlite only supports compact write mode (we have schema there)

            builder.WithJsonOptions(options => options.WriteMode = JsonWriteMode.Compact);

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
