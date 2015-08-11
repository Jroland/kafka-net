using System;
using System.Diagnostics;

namespace KafkaNet
{
    public class ConsoleLog : IKafkaLog
    {

        private readonly LogLevel _minLevel;
        public ConsoleLog(LogLevel minLevel)
        {
            _minLevel = minLevel;
        }
        public ConsoleLog()
        {
            _minLevel = LogLevel.Debug;
        }

        private void Log(string message, LogLevel level)
        {
            //%timestamp [%thread] %level %message 
            //ToDO add class!!
            if (level >= _minLevel)
            {

                string logMessge = string.Format("{0} thread:[{1}] level:[{2}] Message:{3}", DateTime.Now,
                    System.Threading.Thread.CurrentThread.ManagedThreadId, level, message);
                Trace.WriteLine(logMessge);
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