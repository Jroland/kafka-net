using System;
using System.Diagnostics;

namespace KafkaNet
{
    /// <summary>
    /// This class simply logs all information out to the Trace log provided by windows.
    /// The reason Trace is being used as the default it to remove extenal references from
    /// the base kafka-net package.  A proper logging framework like log4net is recommended.
    /// </summary>
    public class DefaultTraceLog : IKafkaLog
    {
        private readonly LogLevel _minLevel;

        public DefaultTraceLog(LogLevel minLevel)
        {
            _minLevel = minLevel;
        }

        public DefaultTraceLog()
        {
            _minLevel = LogLevel.Debug;
        }

        private void Log(string message, LogLevel level)
        {
            //%timestamp [%thread] %level %message
            //TODO: static log to each add class!!
            if (level >= _minLevel)
            {
                string logMessage = string.Format("{0} thread:[{1}] level:[{2}] Message:{3}", DateTime.Now.ToString("hh:mm:ss-ffffff"),
                    System.Threading.Thread.CurrentThread.ManagedThreadId, level, message);
                Trace.WriteLine(logMessage);
            }
        }

        public void DebugFormat(string format, params object[] args)
        {
            Log(string.Format(format, args), LogLevel.Debug);
        }

        public void InfoFormat(string format, params object[] args)
        {
            Log(string.Format(format, args), LogLevel.Info);
        }

        public void WarnFormat(string format, params object[] args)
        {
            Log(string.Format(format, args), LogLevel.Warn);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            Log(string.Format(format, args), LogLevel.Error);
        }

        public void FatalFormat(string format, params object[] args)
        {
            Log(string.Format(format, args), LogLevel.Fata);
        }
    }

    public enum LogLevel
    {
        Debug = 0, Info = 1, Warn = 2, Error = 3, Fata = 4
    }
}