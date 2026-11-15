using System.Net;
using System.Net.Sockets;

namespace PgOutput2Json.Benchmark
{
    /// <summary>
    /// A webhook in the process of the benchmark: it reads every batch and answers 200, so the time is that of the output.
    /// </summary>
    internal sealed class WebhookReceiver : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Task _loop;

        public string Url { get; }

        public WebhookReceiver()
        {
            Url = $"http://127.0.0.1:{FreePort()}/";

            _listener.Prefixes.Add(Url);
            _listener.Start();

            _loop = Task.Run(ReceiveAsync);
        }

        private async Task ReceiveAsync()
        {
            while (_listener.IsListening)
            {
                HttpListenerContext context;

                try
                {
                    context = await _listener.GetContextAsync();
                }
                catch (Exception) when (!_listener.IsListening)
                {
                    return;
                }

                await context.Request.InputStream.CopyToAsync(Stream.Null);

                context.Response.StatusCode = 200;
                context.Response.Close();
            }
        }

        private static int FreePort()
        {
            using var socket = new TcpListener(IPAddress.Loopback, 0);

            socket.Start();

            return ((IPEndPoint)socket.LocalEndpoint).Port;
        }

        public void Dispose()
        {
            _listener.Stop();
            _listener.Close();
        }
    }
}
