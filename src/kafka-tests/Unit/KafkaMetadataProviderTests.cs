using System.Collections.Generic;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using NSubstitute;

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

        [Test]
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

        [Test]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        public void ShouldBackoffRequestOnMultipleFailures(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>())
                .Returns(   x => CreateMetadataResponse(errorCode),
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

        [Test]
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

        [Test]
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



        [Test]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.RequestTimedOut)]
        [TestCase(ErrorResponseCode.InvalidMessage)]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(errorCode));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, host, 1));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
            }
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        [ExpectedException(typeof(InvalidTopicMetadataException))]
        public void ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IKafkaConnection>();

            conn.SendAsync(Arg.Any<IKafkaRequest<MetadataResponse>>()).Returns(x => CreateMetadataResponse(1, "123", port));

            using (var provider = new KafkaMetadataProvider(_log))
            {
                var response = provider.Get(new[] { conn }, new[] { "Test" });
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
