using System;

namespace KafkaNet
{
    /// <summary>
    /// This class simply logs all information out to the console. Usefull for 
    /// debug testing in console applications.
    /// </summary>
    public class ConsoleLog : IKafkaLog
    {
        public void DebugFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void InfoFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void WarnFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void FatalFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }
}