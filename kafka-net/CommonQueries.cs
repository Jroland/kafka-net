using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class CommonQueries : IDisposable
    {
        private readonly IBrokerRouter _brokerRouter;

        public CommonQueries(IBrokerRouter brokerRouter)
        {
            _brokerRouter = brokerRouter;
        }

        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="topic">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public Task<List<OffsetResponse>> GetTopicOffset(string topic, int maxOffsets = 1, int time = -1)
        {
            var topicMetadata = GetTopic(topic);

            var offsets = new List<Offset>(topicMetadata.Partitions.Select(x => new Offset
                                {
                                    Topic = topic,
                                    PartitionId = x.PartitionId,
                                    MaxOffsets = maxOffsets,
                                    Time = time
                                }));

            var offsetRequest = new OffsetRequest { Offsets = offsets };

            var route = _brokerRouter.SelectBrokerRoute(topic);
            return route.Connection.SendAsync(offsetRequest);
        }

        /// <summary>
        /// Get metadata on the given topic.
        /// </summary>
        /// <param name="topic">The metadata on the requested topic.</param>
        /// <returns>Topic object containing the metadata on the requested topic.</returns>
        public Topic GetTopic(string topic)
        {
            var response = _brokerRouter.GetTopicMetadata(topic);

            if (response.Count <= 0) throw new InvalidTopicMetadataException(string.Format("No metadata could be found for topic: {0}", topic));

            return response.First();
        }


        public void Dispose()
        {
            using (_brokerRouter) { }
        }
    }
}
