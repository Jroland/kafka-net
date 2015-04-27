namespace StatisticsTestLoader
{
    public class KafkaRecord
    {
        public string Key { get; set; }
        public string Topic { get; set; }
        public long Offset { get; set; }
        public string Record { get; set; }
    }
}