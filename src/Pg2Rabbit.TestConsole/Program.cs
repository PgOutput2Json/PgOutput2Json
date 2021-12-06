// See https://aka.ms/new-console-template for more information
using Pg2Rabbit.Core;

Console.WriteLine("Hello, World!");

await ReplicationListener.ListenForChanges();
