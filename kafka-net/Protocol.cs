using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;


namespace KafkaNet
{
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        LeaderAndIsr = 4,
        StopReplica = 5,
        OffsetCommit = 6,
        OffsetFetch = 7
    }

    public enum ErrorResponseCode
    {
        NoError = 0,
        Unknown = -1, 
        OffsetOutOfRange =1,
        InvalidMessage = 2,
        UnknownTopicOrPartition = 3,
        InvalidMessageSize = 4,
        LeaderNotAvailable = 5,
        NotLeaderForPartition = 6,
        RequestTimedOut = 7,
        BrokerNotAvailable = 8,
        ReplicaNotAvailable = 9,
        MessageSizeTooLarge = 10,
        StaleControllerEpochCode = 11,
        OffsetMetadataTooLargeCode = 12
    }
    
    /// <summary>
    /// Kafka Protocol implementation:
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public static class Protocol1
    {
        

        

       
       

       

        
    }
}
