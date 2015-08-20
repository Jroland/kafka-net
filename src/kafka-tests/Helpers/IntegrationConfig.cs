using KafkaNet;
using System;
using System.Configuration;

namespace kafka_tests.Helpers
{
    public static class IntegrationConfig
    {
        public static string IntegrationCompressionTopic = "IntegrationCompressionTopic";
        public static string IntegrationTopic = "IntegrationTopic";
        public static string IntegrationConsumer = "IntegrationConsumer";
        public const int NumberOfRepeat = 1;
        public static IKafkaLog NoDebugLog = new DefaultTraceLog(LogLevel.Info);// Some of the tests measured performance.my log is too slow so i change the log level to only critical  message
        public static IKafkaLog AllLog = new DefaultTraceLog();

        public static string Highlight(string message)
        {
            return String.Format("**************************{0}**************************", message);
        }

        public static string Highlight(string message, params object[] args)
        {
            return String.Format("**************************{0}**************************", string.Format(message, args));
        }

        public static Uri IntegrationUri
        {
            get
            {
                var url = ConfigurationManager.AppSettings["IntegrationKafkaServerUrl"];
                if (url == null) throw new ConfigurationErrorsException("IntegrationKafkaServerUrl must be specified in the app.config file.");
                return new Uri(url);
            }
        }
    }
}