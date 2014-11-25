using System;
using System.Diagnostics;
using System.Threading;

namespace kafka_tests.Helpers
{
    public static class TaskTest
    {
        public static bool WaitFor(Func<bool> predicate, int milliSeconds = 3000)
        {
            var sw = Stopwatch.StartNew();
            while (predicate() != true)
            {
                if (sw.ElapsedMilliseconds > milliSeconds)
                    return false;
                Thread.Sleep(500);
            }
            return true;
        }
        
        public static string ToString(this KafkaNet.Protocol.Message msg){
        	        	return string.Format("[Message Meta={0}, MagicNumber={1}, Attribute={2}, Key={3}, Value={4}]", 
        	                     msg.Meta, msg.MagicNumber, msg.Attribute, msg.Key==null? "null" : System.Text.Encoding.Default.GetString(msg.Key), 
        	                     msg.Value==null? "null" : System.Text.Encoding.Default.GetString(msg.Value));
        }
        
        public static string ToString(this KafkaNet.Protocol.MessageMetadata mata){
			return string.Format("[MessageMetadata Offset={0}, PartitionId={1}]", mata.Offset, mata.PartitionId);
        }
        

    }
}
