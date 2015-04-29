using KafkaNet.Protocol;

namespace StatisticsTestLoader
{
    public class KafkaRecord
    {
        public string Key { get; set; }
        public string Topic { get; set; }
        public long Offset { get; set; }
        public Message Message { get; private set; }

        public void AddDocument(string document)
        {
            Message = new Message(Key, document);
        }
    }
}