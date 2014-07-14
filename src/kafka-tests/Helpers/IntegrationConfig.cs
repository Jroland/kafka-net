using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka_tests.Helpers
{
    public static class IntegrationConfig
    {
        public static string IntegrationCompressionTopic = "IntegrationCompressionTopic";
        public static string IntegrationTopic = "IntegrationTopic";
        public static string IntegrationConsumer = "IntegrationConsumer";
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
