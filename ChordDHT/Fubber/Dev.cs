using ChordDHT.Fubber;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Fubber
{

    public class Dev
    {
        public class LoggerContext : ILogger
        {
            public readonly string Prefix;

            private List<string> Messages;
            private LoggerContext? ParentLogger = null;

            public LoggerContext(string prefix, LoggerContext? parentLogger = default)
            {
                Prefix = prefix;
                ParentLogger = parentLogger;
                Messages = ParentLogger?.Messages ?? new List<string>();
            }

            public List<string>? GetMessages()
            {
                return Messages;
            }

            public void Log(string logLevel, string message, object? values = null, ConsoleColor? color = default)
            {
                lock (Console.Out)
                {
                    string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                    string prefix = $"{timestamp} [{Prefix}] {logLevel} ";
                    var parsedMessage = ParseTemplate(message, values);
                    if (parsedMessage.Contains("\n") || prefix.Length + parsedMessage.Length >= Console.BufferWidth)
                    {
                        WriteLine($"{timestamp} [{Prefix}] {logLevel} {ParseTemplate(message, values)}", color: color);
                    }
                    else
                    {
                        WriteLine($"{timestamp} [{Prefix}] {logLevel} {ParseTemplate(message, values)}", color: color);
                    }
                }
            }

            public LoggerContext Logger(string name)
            {
                return new LoggerContext($"{Prefix}:{name}", this);
            }

            public void WriteLine(string message, object? values = default, ConsoleColor? color = default)
            {
                if (ParentLogger != null)
                {
                    ParentLogger.WriteLine(message, values, color);
                    return;
                }
                var parsedMessage = ParseTemplate(message, values);
                /// Messages.Add(parsedMessage);
                var colorToUse = color ?? Console.ForegroundColor;
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = colorToUse;
                Console.WriteLine(parsedMessage);
                Console.ForegroundColor = oldColor;
            }

            public void Debug(string message, object? values = null)
            {
                Log("DEBUG", message, values, ConsoleColor.Cyan);
            }

            public void Info(string message, object? values = null)
            {
                Log("INFO", message, values, ConsoleColor.Gray);
            }

            public void Notice(string message, object? values = null)
            {
                Log("NOTICE", message, values, ConsoleColor.White);
            }

            public void Warn(string message, object? values = null)
            {
                Log("WARN", message, values, ConsoleColor.DarkYellow);
            }

            public void Error(string message, object? values = null)
            {
                Log("ERROR", message, values, ConsoleColor.Red);
            }

            public void Fatal(string message, object? values = null)
            {
                Log("FATAL", message, values, ConsoleColor.Magenta);
            }

            public void Dump(object o)
            {
                WriteLine(FormatLong(o));
            }
        }

        public static LoggerContext Logger(string name) 
        {
            return new LoggerContext(name);

        }

        private static LoggerContext? _DefaultLogger = null;

        public static LoggerContext DefaultLogger
        {
            get
            {
                if (_DefaultLogger == null)
                {
                    _DefaultLogger = new LoggerContext("");
                }
                return _DefaultLogger;
            }
        }

        public static void Debug(string message, object? values = null) => DefaultLogger.Debug(message, values);

        public static void Info(string message, object? values = null) => DefaultLogger.Info(message, values);

        public static void Notice(string message, object? values = null) => DefaultLogger.Notice(message, values);

        public static void Warn(string message, object? values = null) => DefaultLogger.Warn(message, values);

        public static void Error(string message, object? values = null) => DefaultLogger.Error(message, values);

        public static void Dump(object o) => DefaultLogger.Dump(o);

        public static void WriteLine(string message, object? values = default) => DefaultLogger.WriteLine(message, values);


        public static string ParseTemplate(string template, object? values = default)
        {
            if (values == null)
            {
                return template;
            }

            foreach (PropertyInfo prop in values.GetType().GetProperties())
            {
                template = template.Replace($"{{{prop.Name}}}", prop.GetValue(values)?.ToString() ?? "[undefined]");
            }

            return template;
        }

        public static string FormatShort(object? o, bool detailed = false, int maxDepth = 2, HashSet<object>? visited = null)
        {

            if (o is string)
            {
                return JsonSerializer.Serialize(o);
            }

            if (visited == null) visited = new HashSet<object>();

            if (maxDepth < 0)
            {
                return "[Max Recursion Depth]";
            }

            if (o == null)
            {
                return "null";
            }

            if (visited.Contains(o))
            {
                return $"[RECURSION*{o.GetType().Name}]";
            }
            if (!(o is string))
            {
                visited.Add(o);
            }

            TypeCode typeCode = Type.GetTypeCode(o.GetType());

            string enumerablePreview = "";
            if (o is IEnumerable enumerable && !(o is string))
            {
                var firstThree = enumerable.Cast<object>().Take(3).Select(e => FormatShort(e)).ToList();
                if (firstThree.Count == 3)
                {
                    firstThree.Add("...trimmed");
                }
                enumerablePreview = string.Join(", ", firstThree);
                enumerablePreview = $" {{{enumerablePreview}}}";
            }

            if (typeCode != TypeCode.Object || typeCode == TypeCode.String)
            {
                return $"{o}{enumerablePreview} ({o.GetType().Name})";
            }

            if (detailed)
            {
                var properties = o.GetType().GetProperties()
                                    .Select(p => $"{p.Name}={FormatShort(p.GetValue(o), false, maxDepth - 1, visited)}")
                                    .ToList();

                return $"[{o.GetType().Name}{enumerablePreview} #{o.GetHashCode()}({string.Join(',', properties)})]";
            }
            else
            {
                return $"[{o.GetType().Name}{enumerablePreview} #{o.GetHashCode()}]";
            }
        }

        public static string FormatLong(object? o, int maxDepth = 2, HashSet<object>? visited = null, string indent = "")
        {
            if (visited == null) visited = new HashSet<object>();

            if (maxDepth <= 0)
            {
                return FormatShort(o, false, maxDepth, visited);
            }

            if (o == null)
            {
                return indent + "null\n";
            }

            if (visited.Contains(o))
            {
                return indent + $"[RECURSION*{o.GetType().Name} #{o.GetHashCode()}]\n";
            }
            if (!(o is string))
            {
                visited.Add(o);
            }

            StringBuilder sb = new StringBuilder();
            sb.AppendLine(indent + FormatShort(o, false));

            // Skip examining properties if 'o' is a string
            if (o is string)
            {
                return sb.ToString();
            }

            string deeperIndent = indent + "  ";
            foreach (var prop in o.GetType().GetProperties())
            {
                if (prop.GetIndexParameters().Length > 0)
                {
                    continue;
                }
                object? propValue;

                string extraInfo = "";
                var getter = prop.GetMethod;
                var setter = prop.SetMethod;

                if (getter != null)
                {
                    extraInfo += getter.IsDefined(typeof(CompilerGeneratedAttribute)) ? "" : "getter";
                }

                if (setter != null)
                {
                    if (!string.IsNullOrEmpty(extraInfo)) extraInfo += ", ";
                    extraInfo += setter.IsDefined(typeof(CompilerGeneratedAttribute)) ? "" : "setter";
                }

                if (string.IsNullOrEmpty(extraInfo) && !prop.CanWrite)
                {
                    if (!string.IsNullOrEmpty(extraInfo)) extraInfo += ", ";
                    extraInfo += "readonly";
                }

                try
                {
                    propValue = prop.GetValue(o);
                }
                catch (Exception ex)
                {
                    propValue = $"[Error: {ex.Message}]";
                }
                string formattedValue = FormatLong(propValue, maxDepth - 1, visited, deeperIndent).TrimEnd().TrimStart();
                sb.AppendLine($"{deeperIndent}{prop.Name}{(string.IsNullOrEmpty(extraInfo) ? "" : $" ({extraInfo})")}: {formattedValue}");
            }

            return sb.ToString();
        }
    }
}
