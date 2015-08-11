using System;
using System.Net;
using KafkaNet;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class KafkaEndpointTests
    {
        private  IKafkaLog _log=new ConsoleLog(LogLevel.Warn);
        [Test]
        public void EnsureEndpointCanBeResulved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            Assert.That(endpoint.Endpoint.Address, Is.EqualTo(expected));
            Assert.That(endpoint.Endpoint.Port, Is.EqualTo(8888));
        }

        [Test]
        public void EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            var endpoint2 = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);

            Assert.That(ReferenceEquals(endpoint1, endpoint2), Is.False, "Should not be the same reference.");
            Assert.That(endpoint1, Is.EqualTo(endpoint2));
        }

        [Test]
        public void EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:8888"), _log);
            var endpoint2 = new DefaultKafkaConnectionFactory().Resolve(new Uri("http://localhost:1"), _log);

            Assert.That(endpoint1, Is.Not.EqualTo(endpoint2));
        }
    }
}
