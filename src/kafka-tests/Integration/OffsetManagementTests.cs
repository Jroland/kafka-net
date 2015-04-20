﻿using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class OffsetManagementTests
    {
        private readonly KafkaOptions Options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void OffsetFetchRequestOfNonExistingGroupShouldReturnNoError([Values(0,1)] int version)
        {
            //From documentation: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+ProtocolTests#AGuideToTheKafkaProtocol-OffsetFetchRequest
            //Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code 
            //(since it is not really an error), but returns empty metadata and sets the offset field to -1.
            const int partitionId = 0;
            using (var router = new BrokerRouter(Options))
            {
                var request = CreateOffsetFetchRequest(version, Guid.NewGuid().ToString(), partitionId);

                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

                var response = conn.Connection.SendAsync(request).Result.FirstOrDefault();

                Assert.That(response, Is.Not.Null);
                if (version == 0)
                {
                    // Version 0 (storing in zookeeper) results in unknown topic or partition as the consumer group
                    // and partition are used to make up the string, and when it is missing it results in an error
                    Assert.That(response.Error, Is.EqualTo((int)ErrorResponseCode.UnknownTopicOrPartition));
                }
                else
                {
                    Assert.That(response.Error, Is.EqualTo((int)ErrorResponseCode.NoError));
                }
                Assert.That(response.Offset, Is.EqualTo(-1));
            }
        }

        [Test]
        public void OffsetCommitShouldStoreAndReturnSuccess([Values(0, 1)] int version)
        {
            const int partitionId = 0;
            using (var router = new BrokerRouter(Options))
            {
                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

                var commit = CreateOffsetCommitRequest(version, IntegrationConfig.IntegrationConsumer, partitionId, 10);
                var response = conn.Connection.SendAsync(commit).Result.FirstOrDefault();

                Assert.That(response, Is.Not.Null);
                Assert.That(response.Error, Is.EqualTo((int)ErrorResponseCode.NoError));
            }
        }

        [Test]
        public void OffsetCommitShouldStoreOffsetValue([Values(0, 1)] int version)
        {
            const int partitionId = 0;
            const long offset = 99;

            using (var router = new BrokerRouter(Options))
            {

                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

                var commit = CreateOffsetCommitRequest(version, IntegrationConfig.IntegrationConsumer, partitionId, offset);
                var commitResponse = conn.Connection.SendAsync(commit).Result.FirstOrDefault();
                
                Assert.That(commitResponse, Is.Not.Null);
                Assert.That(commitResponse.Error, Is.EqualTo((int)ErrorResponseCode.NoError));

                var fetch = CreateOffsetFetchRequest(version, IntegrationConfig.IntegrationConsumer, partitionId);
                var fetchResponse = conn.Connection.SendAsync(fetch).Result.FirstOrDefault();

                Assert.That(fetchResponse, Is.Not.Null);
                Assert.That(fetchResponse.Error, Is.EqualTo((int)ErrorResponseCode.NoError));
                Assert.That(fetchResponse.Offset, Is.EqualTo(offset));
            }
        }

        [Test]
        public void OffsetCommitShouldStoreMetadata([Values(0, 1)] int version)
        {
            const int partitionId = 0;
            const long offset = 101;
            const string metadata = "metadata";

            using (var router = new BrokerRouter(Options))
            {
                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic, partitionId);

                var commit = CreateOffsetCommitRequest(version, IntegrationConfig.IntegrationConsumer, partitionId, offset, metadata);
                var commitResponse = conn.Connection.SendAsync(commit).Result.FirstOrDefault();

                Assert.That(commitResponse, Is.Not.Null);
                Assert.That(commitResponse.Error, Is.EqualTo((int)ErrorResponseCode.NoError));

                var fetch = CreateOffsetFetchRequest(version, IntegrationConfig.IntegrationConsumer, partitionId);
                var fetchResponse = conn.Connection.SendAsync(fetch).Result.FirstOrDefault();

                Assert.That(fetchResponse, Is.Not.Null);
                Assert.That(fetchResponse.Error, Is.EqualTo((int)ErrorResponseCode.NoError));
                Assert.That(fetchResponse.Offset, Is.EqualTo(offset));

                // metadata is only stored with version 1. Zookeeper doesn't store metadata
                if (version == 1)
                {
                    Assert.That(fetchResponse.MetaData, Is.EqualTo(metadata));
                }
            }
        }

        [Test]
        public void ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = new BrokerRouter(Options))
            {
                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic);

                var request = new ConsumerMetadataRequest {ConsumerGroup = IntegrationConfig.IntegrationConsumer};

                var response = conn.Connection.SendAsync(request).Result.FirstOrDefault();

                Assert.That(response, Is.Not.Null);
                Assert.That(response.Error, Is.EqualTo((int)ErrorResponseCode.NoError));
            }
        }

        private OffsetFetchRequest CreateOffsetFetchRequest(int version, string consumerGroup, int partitionId)
        {
            var request = new OffsetFetchRequest((short)version)
            {
                ConsumerGroup = consumerGroup,
                Topics = new List<OffsetFetch>
                    {
                        new OffsetFetch
                        {
                            PartitionId = partitionId,
                            Topic = IntegrationConfig.IntegrationTopic
                        }
                    }
            };

            return request;
        }

        private OffsetCommitRequest CreateOffsetCommitRequest(int version, string consumerGroup, int partitionId, long offset, string metadata = null)
        {
            var commit = new OffsetCommitRequest((short)version)
            {
                ConsumerGroup = consumerGroup,
                OffsetCommits = new List<OffsetCommit>
                            {
                                new OffsetCommit
                                    {
                                        PartitionId = partitionId,
                                        Topic = IntegrationConfig.IntegrationTopic,
                                        Offset = offset,
                                        Metadata = metadata
                                    }
                            }
            };

            return commit;
        }
    }
}
