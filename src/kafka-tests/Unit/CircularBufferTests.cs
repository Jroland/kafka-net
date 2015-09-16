﻿using kafka_tests.Helpers;
using KafkaNet.Common;
using NUnit.Framework;
using System.Linq;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class CircularBufferTests
    {
        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BufferShouldOnlyStoreMaxAmount()
        {
            var buffer = new ConcurrentCircularBuffer<int>(2);

            for (int i = 0; i < 10; i++)
            {
                buffer.Enqueue(i);
            }

            Assert.That(buffer.Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BufferShouldCountUntilMaxHitThenAlswaysShowMax()
        {
            var buffer = new ConcurrentCircularBuffer<int>(2);

            Assert.That(buffer.Count, Is.EqualTo(0));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(1));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(2));
            buffer.Enqueue(1);
            Assert.That(buffer.Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void BufferMaxSizeShouldReportMax()
        {
            var buffer = new ConcurrentCircularBuffer<int>(2);

            Assert.That(buffer.MaxSize, Is.EqualTo(2));
            buffer.Enqueue(1);
            Assert.That(buffer.MaxSize, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnumerationShouldReturnOnlyRecordsWithData()
        {
            var buffer = new ConcurrentCircularBuffer<int>(2);
            Assert.That(buffer.ToList().Count, Is.EqualTo(0));

            buffer.Enqueue(1);
            Assert.That(buffer.ToList().Count, Is.EqualTo(1));

            buffer.Enqueue(1);
            buffer.Enqueue(1);
            Assert.That(buffer.ToList().Count, Is.EqualTo(2));
        }

        [Test, Repeat(IntegrationConfig.NumberOfRepeat)]
        public void EnqueueShouldAddToFirstSlot()
        {
            var buffer = new ConcurrentCircularBuffer<int>(2);
            buffer.Enqueue(1);
            Assert.That(buffer.First(), Is.EqualTo(1));
        }
    }
}