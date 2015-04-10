using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Protocol;
using kafka_tests.Helpers;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolTests
    { 
        [Test]
        public void MetadataResponseShouldDecode()
        {
            var request = new MetadataRequest();
            var response = request.Decode(MessageHelper.CreateMetadataResponse(1, "Test").Skip(4).ToArray()).First();

            Assert.That(response.CorrelationId, Is.EqualTo(1));
            Assert.That(response.Topics[0].Name, Is.EqualTo("Test"));
        }
    }
}
