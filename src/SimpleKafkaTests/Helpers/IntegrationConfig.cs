using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Helpers
{
    public static class IntegrationConfig
    {
//        public static string IntegrationConsumer = "IntegrationConsumer";
//        public static Uri IntegrationUri = new Uri("http://server.home:9092");
//        public static Uri IntegrationUri = new Uri("http://server.home:39093");

        public static Uri IntegrationUriOld
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
