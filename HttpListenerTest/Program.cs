// See https://aka.ms/new-console-template for more information
using System.Net;
using System.Text;

HttpListener listener1 = new HttpListener();
listener1.Prefixes.Add("http://localhost:8088/listener1/");
listener1.Start();

HttpListener listener2 = new HttpListener();
listener2.Prefixes.Add("http://localhost:8088/listener2/");
listener2.Start();

var tasks = new List<Task>();
tasks.Add(Task.Run(async () =>
{
    do
    {
        Console.WriteLine("Waiting on listener1");
        var context = await listener1.GetContextAsync();
        Console.WriteLine($"listener1: {context.Request.HttpMethod} {context.Request.RawUrl}");
        context.Response.StatusCode = 200;
        await context.Response.OutputStream.WriteAsync(Encoding.UTF8.GetBytes("Hello from listener 1"));
        context.Response.Close();
    } while (true);
}));
tasks.Add(Task.Run(async () =>
{
    do
    {
        Console.WriteLine("Waiting on listener2");
        var context = await listener2.GetContextAsync();
        Console.WriteLine($"listener2: {context.Request.HttpMethod} {context.Request.RawUrl}");
        context.Response.StatusCode = 200;
        await context.Response.OutputStream.WriteAsync(Encoding.UTF8.GetBytes("Hello from listener 2"));
        context.Response.Close();
    } while (true);
}));
Task.WaitAll(tasks.ToArray());