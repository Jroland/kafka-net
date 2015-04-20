using NUnit.Framework;
using SimpleKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafkaTests.Unit
{
    [TestFixture]
    [Category("Unit")]
    class PartitionerTests
    {
        [Theory]
        [TestCase("", 1, 0)]
        [TestCase("Hello1", 1, 0)]
        [TestCase("Hello2", 2, 1)]
        public void TestStringPartitioner(string stringToTest, int numberOfPartitions, int partitionExpected)
        {
            var partitioner = new StringPartitioner();
            var result = partitioner.CalculatePartition(stringToTest, numberOfPartitions);
            Assert.That(result, Is.EqualTo(partitionExpected));
        }

        [Theory]
        [TestCase("", 1, 0)]
        [TestCase("Hello1", 1, 0)]
        [TestCase("Hello4", 2, 1)]
        public void TestFixedPartitioner(object objectToTest, int numberOfPartitions, int partitionExpected)
        {
            var partitioner = new FixedPartitioner<object>(partitionExpected);
            var result = partitioner.CalculatePartition(objectToTest, numberOfPartitions);
            Assert.That(result, Is.EqualTo(partitionExpected));
        }

        [Theory]
        [TestCase("Test", 1)]
        [TestCase("Test", 2)]
        public void TestLoadBalancedPartitioner(object objectToTest, int numberOfPartitions)
        {
            var partitioner = new LoadBalancedPartitioner<object>();
            var expected = 0;
            for (var i = 0; i < numberOfPartitions * 2; i++ )
            {
                var result = partitioner.CalculatePartition(objectToTest, numberOfPartitions);
                Assert.That(result, Is.EqualTo(expected));
                expected = (expected + 1) % numberOfPartitions;
            }
        }
    }
}
