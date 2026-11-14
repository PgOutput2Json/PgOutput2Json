using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json.ClickHouse
{
    /// <summary>
    /// The HTTP interface of ClickHouse: a statement in the URL, and for an insert the rows in the body.
    /// </summary>
    internal sealed class ClickHouseClient
    {
        private readonly ClickHousePublisherOptions _options;
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;

        public ClickHouseClient(ClickHousePublisherOptions options, HttpClient httpClient)
        {
            _options = options;
            _httpClient = httpClient;

            var url = new StringBuilder(options.Endpoint.TrimEnd('/'));

            url.Append("/?database=").Append(Uri.EscapeDataString(options.Database));

            // timestamptz values come with the offset ("+00"), which the default parser does not read
            url.Append("&date_time_input_format=best_effort");

            // set either way: the default of the server depends on its version
            url.Append(options.UseAsyncInserts ? "&async_insert=1&wait_for_async_insert=1" : "&async_insert=0");

            foreach (var setting in options.Settings)
            {
                url.Append('&').Append(Uri.EscapeDataString(setting.Key)).Append('=').Append(Uri.EscapeDataString(setting.Value));
            }

            _baseUrl = url.ToString();
        }

        /// <summary>
        /// Runs a statement and returns what ClickHouse answered.
        /// </summary>
        /// <param name="query">The statement. An insert ends with its FORMAT, and its rows are in <paramref name="data"/>.</param>
        /// <param name="parameters">Values of the query parameters ({name:Type} in the statement).</param>
        public async Task<string> ExecuteAsync(string query, string? data, IEnumerable<KeyValuePair<string, string>>? parameters, CancellationToken token)
        {
            var url = new StringBuilder(_baseUrl);

            HttpContent content;

            if (data != null)
            {
                url.Append("&query=").Append(Uri.EscapeDataString(query));

                content = new StringContent(data, Encoding.UTF8);
            }
            else
            {
                content = new StringContent(query, Encoding.UTF8);
            }

            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    url.Append("&param_").Append(parameter.Key).Append('=').Append(Uri.EscapeDataString(parameter.Value));
                }
            }

            using var request = new HttpRequestMessage(HttpMethod.Post, url.ToString()) { Content = content };

            request.Headers.Add("X-ClickHouse-User", _options.Username);
            request.Headers.Add("X-ClickHouse-Key", _options.Password);

            using var response = await _httpClient.SendAsync(request, token).ConfigureAwait(false);

            var body = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);

            // an error found after the response started has status 200, and the code in this header
            if (!response.IsSuccessStatusCode || response.Headers.Contains("X-ClickHouse-Exception-Code"))
            {
                var code = response.Headers.TryGetValues("X-ClickHouse-Exception-Code", out var codes) ? codes.FirstOrDefault() : null;

                throw new ClickHouseException((int)response.StatusCode, code, Shorten(query), Shorten(body.Trim()));
            }

            return body;
        }

        private static string Shorten(string text) => text.Length <= 1000 ? text : string.Concat(text.AsSpan(0, 1000), "...");
    }

    public class ClickHouseException : Exception
    {
        public int StatusCode { get; }

        /// <summary>
        /// The ClickHouse error code, e.g. "60" for a table that does not exist.
        /// </summary>
        public string? Code { get; }

        public ClickHouseException(int statusCode, string? code, string query, string error)
            : base($"ClickHouse answered {statusCode}{(code != null ? $" (code {code})" : "")} to \"{query}\": {error}")
        {
            StatusCode = statusCode;
            Code = code;
        }
    }
}
