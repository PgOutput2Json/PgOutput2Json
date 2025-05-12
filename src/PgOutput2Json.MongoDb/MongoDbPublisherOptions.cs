using System;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace PgOutput2Json.MongoDb
{
    public class MongoDbPublisherOptions
    {
        public required MongoClientSettings ClientSettings { get; set; }
        public required string DatabaseName { get; set; }

        public Func<MongoClient, Task>? PostConnectionSetup { get; set; }

    }
}
