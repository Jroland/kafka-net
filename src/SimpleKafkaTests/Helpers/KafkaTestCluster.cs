using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Helpers
{
    internal class KafkaTestCluster : IDisposable
    {
        private class ProcessMonitor
        {
            private readonly Process process;
            private readonly StringBuilder stdout = new StringBuilder();
            private readonly StringBuilder stderr = new StringBuilder();

            public string Stdout { get { return stdout.ToString(); } }
            public string Stderr { get { return stderr.ToString();  } }

            public ProcessMonitor(Process process)
            {
                this.process = process;
                process.OutputDataReceived += (_, e) => stdout.AppendLine(e.Data);
                process.ErrorDataReceived += (_, e) => stderr.AppendLine(e.Data);
                process.BeginErrorReadLine();
                process.BeginOutputReadLine();
            }

            public void WaitForExit()
            {
                process.WaitForExit();
            }

            public bool WaitForExit(int milliseconds)
            {
                return process.WaitForExit(milliseconds);
            }

            public bool HasExited { get { return process.HasExited; } }
        }
        private readonly ProcessMonitor zookeeperProcess;
        private readonly string dockerHost;
        private readonly int dockerPort;
        private readonly int portBase;
        private readonly ProcessMonitor[] kafkaProcesses;

        public Uri[] CreateBrokerUris()
        {
            return Enumerable
                .Range(0, kafkaProcesses.Length)
                .Select(brokerId => new Uri("http://" + dockerHost + ":" + (portBase + 1 + brokerId)))
                .ToArray();
        }

        public KafkaTestCluster(string dockerHost, int brokerCount = 1, int portBase = 45678, int dockerPort = 2375)
        {
            this.dockerHost = dockerHost;
            this.portBase = portBase;
            this.dockerPort = dockerPort;
            DestroyContainers(brokerCount);
            this.zookeeperProcess = StartZookeeper();
            this.kafkaProcesses = StartKafkaBrokers(brokerCount);
        }

        public void StopKafkaBroker(int brokerId)
        {
            StopKafka(brokerId);
            kafkaProcesses[brokerId] = null;
        }

        public void RestartKafkaBroker(int brokerId)
        {
            var process = RunAndCheckDocker("start", "-a", GetKafkaName(brokerId));
            kafkaProcesses[brokerId] = process;
        }

        private void RunTopicCommand(params object[] args)
        {
            var process = RunDocker("run", "--rm", KafkaImage, "bin/kafka-topics.sh", "--zookeeper", dockerHost + ":" + portBase,
                String.Join(" ", args));
            process.WaitForExit();
            if (!string.IsNullOrWhiteSpace(process.Stdout))
            {
                Console.WriteLine("Stdout: {0}", process.Stdout);
            }
            if (!string.IsNullOrWhiteSpace(process.Stderr))
            {
                Console.WriteLine("Stderr: {0}", process.Stderr);
            }
        }

        public class DisposableTopic : IDisposable
        {
            private readonly KafkaTestCluster cluster;
            private readonly string name;
            public string Name { get { return name; } }
            public DisposableTopic(KafkaTestCluster cluster, 
                string name, int partitions = 1, int replicationFactor = 1)
            {
                this.cluster = cluster;
                this.name = name;
                cluster.CreateTopic(name, partitions, replicationFactor);
            }

            public void Dispose()
            {
                cluster.DeleteTopic(name);

            }
        }
 
        public DisposableTopic CreateTemporaryTopic(int partitions = 1, int replicationFactor = 1) {
            return new DisposableTopic(this, Guid.NewGuid().ToString(), partitions, replicationFactor);
        }
        

        public void CreateTopic(string topicName, int partitions = 1, int replicationFactor = 1) {
            RunTopicCommand("--topic", topicName, "--create", "--partitions", partitions, "--replication-factor", replicationFactor);
        }

        public void DeleteTopic(string topicName)
        {
            RunTopicCommand("--topic", topicName, "--delete");
        }

        private ProcessMonitor RunAndCheckDocker(params object[] args)
        {
            var process = RunDocker(args);
            var exited = process.WaitForExit(1000);
            if (exited)
            {
                var stdout = process.Stdout;
                var stderr = process.Stderr;
                throw new InvalidOperationException("Failed to run\nStdout: " + stdout + "\nStderr: " + stderr);
            }
            else
            {
                return process;
            }
        }
        private ProcessMonitor RunDocker(params object[] args)
        {
            var arguments = string.Format(CultureInfo.InvariantCulture, "--host=tcp://{0}:{1} {2}",
                dockerHost,
                dockerPort,
                String.Join(" ", args)
                );

            var info = new ProcessStartInfo
            {
                Arguments = arguments,
                CreateNoWindow = true,
                FileName = "docker.exe",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            var process = Process.Start(info);
            return new ProcessMonitor(process);
        }

        private ProcessMonitor[] StartKafkaBrokers(int brokerCount)
        {
            return
                Enumerable
                    .Range(0, brokerCount)
                    .Select(StartKafka)
                    .ToArray();
        }

        private const string KafkaImage = "sceneskope/kafka:0.8.2.1-1";

        private ProcessMonitor StartKafka(int brokerId)
        {
            var port = portBase + 1 + brokerId;
            return RunAndCheckDocker("run",
                "--name", GetKafkaName(brokerId),
                "--publish", port + ":9092",
                "--env", "KAFKA_BROKER_ID=" + brokerId,
                "--env", "KAFKA_ADVERTISED_HOST_NAME=" + dockerHost,
                "--env", "KAFKA_ADVERTISED_PORT=" + port,
                "--env", "KAFKA_ZOOKEEPER_CONNECT=" + dockerHost + ":" + portBase,
                "--env", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
                "--env", "KAFKA_DELETE_TOPIC_ENABLE=true",
                KafkaImage
            );
        }

        private void StopKafka(int brokerId)
        {
            StopAndWaitForContainer(GetKafkaName(brokerId), kafkaProcesses[brokerId]);
            Console.WriteLine("KOut[{0}] = {1}\n, KErr[{0}] = {2}", brokerId, kafkaProcesses[brokerId].Stdout, kafkaProcesses[brokerId].Stderr);
        }

        private void StopKafkaBrokers()
        {
            for (var i = 0; i < kafkaProcesses.Length; i++)
            {
                StopKafka(i);
            }
        }

        private string GetKafkaName(int brokerId) { return "kTest_" + portBase + "_" + brokerId;  }
        private string GetZookeeperName() { return "zkTest_" + portBase; }
        private ProcessMonitor StartZookeeper()
        {
            return RunAndCheckDocker("run", "--rm",
            "--name", GetZookeeperName(),
            "--publish", portBase + ":2181",
            "--env", "JMXDISABLE=true",
            "sceneskope/zookeeper:3.4.6");
        }

        private void StopZookeeper()
        {
            StopAndWaitForContainer(GetZookeeperName(), zookeeperProcess);
            Console.WriteLine("ZKOut = {0}\nZkErr = {1}", zookeeperProcess.Stdout, zookeeperProcess.Stderr);
        }

        private void StopAndWaitForContainer(string containerName, ProcessMonitor process)
        {
            if (process.HasExited)
            {
                return;
            }

            var stopCommand = RunDocker("stop", containerName);
            stopCommand.WaitForExit();
            Console.WriteLine("stop {0} = {1},{2}", containerName, stopCommand.Stdout, stopCommand.Stderr);
            var exited = process.WaitForExit(2000);
            if (exited)
            {
                return;
            }

            RunDocker("kill", containerName);
            process.WaitForExit();
        }



        public void Dispose()
        {
            StopKafkaBrokers();
            StopZookeeper();
            DestroyContainers(kafkaProcesses.Length);
        }

        public void DestroyContainers(int brokerCount)
        {
            var args = new List<string> { "rm", "-f", GetZookeeperName() };
            for (var i = 0; i < brokerCount; i++)
            {
                args.Add(GetKafkaName(i));
            }
            var process = RunDocker(args.ToArray());
            process.WaitForExit();
            Console.WriteLine("Destroyed. Out = {0}, Err = {1}", process.Stdout, process.Stderr);
        }
    }
}
