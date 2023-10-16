using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class ContextSender
    {
        private HttpContext Context;

        public ContextSender(HttpContext context)
        {
            Context = context;
        }

        private async Task SendResponse(DataSource source, int statusCode, string contentType)
        {
            Context.Response.StatusCode = statusCode;
            Context.Response.ContentType = contentType;

            if (source.HasKnownLength)
            {
                Context.Response.SendChunked = false;
                Context.Response.ContentLength64 = source.Length;
            } else
            {
                Context.Response.SendChunked = true;
            }

            await source.CopyToAsync(Context.Response.OutputStream);

            // If sending data in chunks, finalize by writing an empty chunk
            if (Context.Response.SendChunked)
            {
                await Context.Response.OutputStream.WriteAsync(new byte[0], 0, 0);
                await Context.Response.OutputStream.FlushAsync();
            }
            Context.Response.Close();
        }

        public async Task Ok(DataSource? source = null, string contentType = "text/plain")
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

        public async Task BadRequest()
        {
            await SendResponse(DataSource.Empty, (int)HttpStatusCode.BadRequest, "text/plain");
        }

        public async Task BadRequest(DataSource reason)
        {
            await SendResponse(reason, (int)HttpStatusCode.BadRequest, "text/plain");
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

        public async Task NotFound(DataSource source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 404, contentType);
        }

        public async Task NotFound(byte[] body, string contentType = "text/plain")
        {
            await NotFound(new DataSource(body), contentType);
        }

        public async Task NotFound(string body, string contentType = "text/plain")
        {
            await NotFound(new DataSource(Encoding.UTF8.GetBytes(body)), contentType);
        }

        public async Task Conflict(DataSource source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 409, contentType);
        }

        public async Task Conflict(byte[] body, string contentType = "text/plain")
        {
            await Conflict(new DataSource(body), contentType);
        }

        public async Task Conflict(string body, string contentType = "text/plain")
        {
            await Conflict(new DataSource(Encoding.UTF8.GetBytes(body)), contentType);
        }

        public async Task InternalServerError(DataSource source = null, string contentType = "text/plain")
        {
            await SendResponse(source ?? DataSource.Empty, 500, contentType);
        }

        public async Task InternalServerError(byte[] body, string contentType = "text/plain")
        {
            await InternalServerError(new DataSource(body), contentType);
        }

        public async Task InternalServerError(string body, string contentType = "text/plain")
        {
            await InternalServerError(new DataSource(Encoding.UTF8.GetBytes(body)), contentType);
        }
    }
}
