using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Fubber.Dev;

namespace ChordDHT.Fubber
{
    public interface ILogger
    {
        /// <summary>
        /// Create a child logger context.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public LoggerContext Logger(string prefix);

        /// <summary>
        /// Log an OK level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Ok(string message, object? values = default);

        /// <summary>
        /// Log a DEBUG level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Debug(string message, object? values = default);

        /// <summary>
        /// Log an INFO level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Info(string message, object? values = default);

        /// <summary>
        /// Log a NOTICE level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Notice(string message, object? values = default);

        /// <summary>
        /// Log a WARN level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Warn(string message, object? values = default);

        /// <summary>
        /// Log an ERROR level message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Error(string message, object? values = default);

        /// <summary>
        /// Log a FATAL level message, usually followed by a termination of the application.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="values"></param>
        public void Fatal(string message, object? values = default);

        public List<string>? GetMessages();
    }
}
