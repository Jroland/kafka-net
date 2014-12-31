using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// This class provides a set of common queries that are useful for both the Consumer and Producer classes.  
    /// </summary>
    public class MetadataQueries : IMetadataQueries
    {
        private readonly IBrokerRouter _brokerRouter;

        public MetadataQueries(IBrokerRouter brokerRouter)
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
        public async Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            var topicMetadata = GetTopic(topic);

            //send the offset request to each partition leader
            var sendRequests = topicMetadata.Partitions
                .GroupBy(x => x.PartitionId)
                .Select(p =>
                    {
                        var route = _brokerRouter.SelectBrokerRoute(topic, p.Key);
                        var request = new OffsetRequest
                                        {
                                            Offsets = new List<Offset>
                                                {
                                                    new Offset
                                                    {
                                                        Topic = topic,
                                                        PartitionId = p.Key,
                                                        MaxOffsets = maxOffsets,
                                                        Time = time
                                                    }
                                                }
                                        };

                        return route.Connection.SendAsync(request);
                    }).ToArray();

            await Task.WhenAll(sendRequests).ConfigureAwait(false);
            return sendRequests.SelectMany(x => x.Result).ToList();
        }

        /// <summary>
        /// Get metadata on the given topic.
        /// </summary>
        /// <param name="topic">The metadata on the requested topic.</param>
        /// <returns>Topic object containing the metadata on the requested topic.</returns>
        public Topic GetTopic(string topic)
        {
            var response = _brokerRouter.GetTopicMetadata(topic);

            if (response.Count <= 0) throw new InvalidTopicMetadataException(ErrorResponseCode.NoError, "No metadata could be found for topic: {0}", topic);

            return response.First();
        }

        public void Dispose()
        {
            using (_brokerRouter) { }
        }
    }
}
