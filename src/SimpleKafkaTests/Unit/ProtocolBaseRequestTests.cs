using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NUnit.Framework;
using SimpleKafka.Protocol;
using SimpleKafka;

namespace SimpleKafkaTests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolBaseRequestTests
    {
        [Test]
        public void EnsureHeaderShouldPackCorrectByteLengths()
        {
            var encoder = new BigEndianEncoder(new byte[14]);
            var request = new FetchRequest { ClientId = "test", CorrelationId = 123456789 };

            BaseRequest.EncodeHeader(request, ref encoder);
            Assert.That(encoder.Offset, Is.EqualTo(14));
            Assert.That(encoder.Buffer, Is.EqualTo(new byte[] { 0, 1, 0, 0, 7, 91, 205, 21, 0, 4, 116, 101, 115, 116 }));
        }
    }
}
