using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleKafka
{
    public enum OffsetSelectionStrategy
    {
        Earliest,
        Latest,
        Specified
    };

    public class TopicSelector
    {
        public string Topic { get; set; }
        public int Partition { get; set; }

        public long Offset { get; set; }

        public OffsetSelectionStrategy DefaultOffsetSelection { get; set; }
        public OffsetSelectionStrategy FailureOffsetSelection { get; set; }

        public TopicSelector()
        {
            DefaultOffsetSelection = OffsetSelectionStrategy.Specified;
            FailureOffsetSelection = OffsetSelectionStrategy.Latest;
        }

    }
}
