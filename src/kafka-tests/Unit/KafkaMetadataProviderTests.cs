using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Protocol;
using NSubstitute;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class KafkaMetadataProviderTests
    {
        private IKafkaLog _log;

        [SetUp]
        public void Setup()
        {
            _log = Substitute.For<IKafkaLog>();
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.OffsetsLoadInProgressCode)]
        [TestCase(ErrorResponseCode.ConsumerCoordinatorNotAvailableCode)]
        public void ShouldRetryWhenReceiveAnRetryErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        public void ShouldBackoffRequestOnMultipleFailures(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(errorCode),
                            x => CreateMetadataResponse(errorCode),
                            x => CreateMetadataResponse(errorCode),
                            x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 400);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 900);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
                _log.WarnFormat("Backing off metadata request retry.  Waiting for {0}ms.", 100);
                conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
            });
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.NoError));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }

            conn.Received(1).SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>());
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public async Task ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, host, 1));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public async Task ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, "123", port));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = await provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        private Task<List<MetadataResponse>> CreateMetadataResponse(int brokerId, string host, int port)
        {
            var tcs = new TaskCompletionSource<List<MetadataResponse>>();
            tcs.SetResult(new List<MetadataResponse>{
                new MetadataResponse
            {
                Brokers = new List<Broker>
                {
                    new Broker
                    {
                        BrokerId = brokerId,
                        Host = host,
                        Port = port}
                },
                Topics  = new List<Topic>()
            }});
            return tcs.Task;
        }

        private Task<List<MetadataResponse>> CreateMetadataResponse(ErrorResponseCode errorCode)
        {
            var tcs = new TaskCompletionSource<List<MetadataResponse>>();
            tcs.SetResult(new List<MetadataResponse>{
                new MetadataResponse
            {
                Brokers = new List<Broker>(),
                Topics = new List<Topic>
                {
                    new Topic
                    {
                        ErrorCode = (short) errorCode,
                        Name = "Test",
                        Partitions = new List<Partition>()
                    }
                }
            }});
            return tcs.Task;
        }
    }
}