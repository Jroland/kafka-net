using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Integration
{
    internal static class IntegrationHelpers
    {

        public static string dockerHost = "tcp://server.home:2375";
        public static string zookeeperHost = "server.home:32181";
        public static string kafkaImage = "sceneskope/kafka:0.8.2.1";
        public static string dockerOptions = "";

        public static void RunKafkaTopicsCommand(params object[] args)
        {
            var cmd = string.Format(CultureInfo.InvariantCulture, "--host={0} run --rm {1} {2} bin/kafka-topics.sh --zookeeper {3} ",
                dockerHost, dockerOptions, kafkaImage, zookeeperHost);

            var arguments = cmd + String.Join(" ", args);

            var info = new ProcessStartInfo
            {
                Arguments = arguments,
                CreateNoWindow = true,
//                FileName = @"c:\users\nick\bin\docker.exe",
                FileName = @"docker.exe",
                UseShellExecute = false,
                RedirectStandardOutput = true,
            };
            var process = Process.Start(info);
            var stdout = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            Console.WriteLine(stdout);
        }

        public static void DeleteTopic(string topic)
        {
            RunKafkaTopicsCommand("--topic", topic, "--delete");
        }

        public static void CreateTopic(string topic, int partitions = 1, int replicationFactor = 1)
        {
            RunKafkaTopicsCommand("--topic", topic, "--create", "--partitions", partitions, "--replication-factor", replicationFactor);
        }

        public static TemporaryTopic CreateTemporaryTopic(int partitions = 1, int replicationFactor = 1)
        {
            return new TemporaryTopic(partitions, replicationFactor);
        }

        public class TemporaryTopic : IDisposable
        {
            private readonly string topic = Guid.NewGuid().ToString();
            public string Topic {  get { return topic;  } }
            public TemporaryTopic(int partitions = 1, int replicationFactor = 1)
            {
                CreateTopic(topic, partitions, replicationFactor);
            }

            public void Dispose()
            {
                DeleteTopic(topic);
            }
        }

    }
}
