using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Protocol;
using NUnit.Framework;

namespace kafka_tests
{
    [TestFixture]
    public class ProtocolTests
    {
        [Test]
        public void EnsureHeaderShouldPackCorrectByteLengths()
        {
            var protocol = new Protocol();
            var result = protocol.EncodeHeader("test", 123456789, 1);

            Assert.That(result.Length, Is.EqualTo(14));
            Assert.That(result, Is.EqualTo(new byte[] { 0, 1, 0, 0, 7, 91, 205, 21, 0, 4, 116, 115, 101, 116 }));
        }
    }
}
