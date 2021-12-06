// See https://aka.ms/new-console-template for more information
using Pg2Rabbit.Core;

var cancellationTokenSource = new CancellationTokenSource();

await ReplicationListener.ListenForChanges(cancellationTokenSource.Token);
