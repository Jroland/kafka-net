using System;
using System.Configuration;
using System.Linq;

namespace StatisticsTestLoader
{
    public class Configuration
    {
        public Uri[] KafkaUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["KafkaUrl"].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => new Uri(x)).ToArray();
            }
        }
        public Uri[] PropertyCacheUrl
        {
            get
            {
                return ConfigurationManager.AppSettings["PropertyCacheUrl"].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => new Uri(x))
                    .ToArray();
            }
        }
        public string CacheUsername {
            get { return ConfigurationManager.AppSettings["CacheUsername"]; }
        }

        public string CachePassword {
            get { return ConfigurationManager.AppSettings["CachePassword"]; }
        }
    }
}