using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Fubber
{
    public class ContextSender
    {
        private HttpContext Context;
        private static long EventIdCounter = 0;

        public ContextSender(HttpContext context)
        {
            Context = context;
        }

        private async Task SendResponse(Stream source, int statusCode, string contentType)
        {
            Context.Response.StatusCode = statusCode;
            Context.Response.ContentType = contentType;

            if (source.CanSeek && source.Length > 0)
            {
                Context.Response.SendChunked = false;
                Context.Response.ContentLength64 = source.Length;
            }
            else
            {
                Context.Response.SendChunked = true;
            }

            byte[] buffer = new byte[4096]; // 4KB buffer, can be adjusted
            int bytesRead;
            while ((bytesRead = await source.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await Context.Response.OutputStream.WriteAsync(buffer, 0, bytesRead);
                await Context.Response.OutputStream.FlushAsync();
            }

            // If sending data in chunks, finalize by writing an empty chunk
            if (Context.Response.SendChunked)
            {
                await Context.Response.OutputStream.WriteAsync(new byte[0], 0, 0);
                await Context.Response.OutputStream.FlushAsync();
            }
            Context.Response.Close();
        }


        public async Task Ok(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 200, contentType);
        }

        public async Task Ok(byte[] body, string contentType = "text/plain")
        {
            await Ok(new DataSource(body), contentType);
        }

        public async Task Ok(string body, string contentType = "text/plain")
        {
            await Ok(new DataSource(body), contentType);
        }

        public async Task JSON(object? data)
        {
            string json = JsonSerializer.Serialize(data);
            await Ok(json, "application/json");
        }

        public async Task BadRequest(Stream? reason = null)
        {
            await SendResponse(reason ?? DataSource.Empty, (int)HttpStatusCode.BadRequest, "text/plain");
        }

        public async Task BadRequest(string reason)
        {
            await SendResponse(new DataSource(reason), (int)HttpStatusCode.BadRequest, "text/plain");
        }

        public async Task BadRequest(byte[] reason)
        {
            await SendResponse(new DataSource(reason), (int)HttpStatusCode.BadRequest, "text/plain");
        }

        public async Task PermanentRedirect(string url)
        {
            Context.Response.StatusCode = 301;
            Context.Response.Headers.Add("Location", url);
            await SendResponse(DataSource.Empty, 301, "text/plain");
        }

        public async Task TemporaryRedirect(string url)
        {
            Context.Response.StatusCode = 302;
            Context.Response.Headers.Add("Location", url);
            await SendResponse(DataSource.Empty, 302, "text/plain");
        }

        public async Task NotFound(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 404, contentType);
        }

        public async Task NotFound(string body, string contentType = "text/plain")
        {
            await NotFound(new DataSource(body), contentType);
        }

        public async Task Conflict(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 409, contentType);
        }

        public async Task Conflict(byte[] body, string contentType = "text/plain")
        {
            await Conflict(new DataSource(body), contentType);
        }

        public async Task Conflict(string body, string contentType = "text/plain")
        {
            await Conflict(new DataSource(body), contentType);
        }

        public async Task InternalServerError(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 500, contentType);
        }

        public async Task InternalServerError(byte[] body, string contentType = "text/plain")
        {
            await InternalServerError(new DataSource(body), contentType);
        }

        public async Task InternalServerError(string body, string contentType = "text/plain")
        {
            await InternalServerError(new DataSource(body), contentType);
        }

        public async Task NotImplemented(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 501, contentType);
        }

        public async Task NotImplemented(string body, string contentType = "text/plain")
        {
            await NotImplemented(new DataSource(body), contentType);
        }

        public async Task Accepted(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 202, contentType);
        }

        public async Task Accepted(string body, string contentType = "text/plain")
        {
            await Accepted(new DataSource(body), contentType);
        }

        public async Task BadGateway(Stream? source = default, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 502, contentType);
        }

        public async Task BadGateway(string body, string contentType = "text/plain")
        {
            await BadGateway(new DataSource(body), contentType);
        }

        public async Task Created(string location, DataSource? source = default, string contentType = "text/plain")
        {
            Context.Response.Headers.Add("Location", location);
            await SendResponse(source ?? DataSource.Empty, 201, contentType);
        }

        public async Task Created(string location, string body, string contentType = "text/plain")
        {
            await Created(location, new DataSource(body), contentType);
        }

        public async Task Forbidden(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 403, contentType);
        }

        public async Task Forbidden(string body, string contentType = "text/plain")
        {
            await Forbidden(new DataSource(body), contentType);
        }

        public async Task Locked(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 423, contentType);
        }

        public async Task Locked(string body, string contentType = "text/plain")
        {
            await Locked(new DataSource(body), contentType);
        }

        public async Task Gone(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 410, contentType);
        }

        public async Task Gone(string body, string contentType = "text/plain")
        {
            await Gone(new DataSource(body), contentType);
        }

        public async Task MethodNotAllowed(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 405, contentType);
        }

        public async Task MethodNotAllowed(string body, string contentType = "text/plain")
        {
            await MethodNotAllowed(new DataSource(body), contentType);
        }

        public async Task Moved(string location)
        {
            Context.Response.StatusCode = 301;
            Context.Response.Headers.Add("Location", location);
            await SendResponse(DataSource.Empty, 301, "text/plain");
        }

        public async Task NotModified()
        {
            await SendResponse(DataSource.Empty, 304, "text/plain");
        }

        public async Task ServiceUnavailable(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 503, contentType);
        }

        public async Task ServiceUnavailable(string body, string contentType = "text/plain")
        {
            await ServiceUnavailable(new DataSource(body), contentType);
        }

        public async Task TooManyRequests(Stream? source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 429, contentType);
        }

        public async Task TooManyRequests(string body, string contentType = "text/plain")
        {
            await TooManyRequests(new DataSource(body), contentType);
        }

        public async Task EventSource(Func<Action<string, object, string?>, Task> handler, int? retryTime = null)
        {
            Context.Response.Headers.Add("Cache-Control", "no-cache");
            Context.Response.ContentType = "text/event-stream";
            Context.Response.StatusCode = 200;

            if (retryTime.HasValue)
            {
                byte[] retryBytes = Encoding.UTF8.GetBytes($"retry: {retryTime}\n");
                await Context.Response.OutputStream.WriteAsync(retryBytes, 0, retryBytes.Length);
                await Context.Response.OutputStream.FlushAsync();
            }

            Action<string, object, string?> sendEvent = async (string eventType, object data, string? id) =>
            {
                string eventId = id ?? Interlocked.Increment(ref EventIdCounter).ToString(); // Atomically increment event ID
                string serializedData = JsonSerializer.Serialize(data);
                string eventPayload = $"id: {eventId}\nevent: {eventType}\ndata: {serializedData}\n\n";

                byte[] eventBytes = Encoding.UTF8.GetBytes(eventPayload);
                await Context.Response.OutputStream.WriteAsync(eventBytes, 0, eventBytes.Length);
                await Context.Response.OutputStream.FlushAsync();
            };

            await handler(sendEvent);
        }


    }
}
