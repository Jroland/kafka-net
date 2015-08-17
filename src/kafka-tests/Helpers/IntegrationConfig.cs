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