/**
 * Created by Administrator on 2017/5/27.
 */

import Bolt.SplitSentenceBolt;
import Bolt.WordCountBolt;
import Spout.SentenceSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @date 2017/5/27
 * @time 16:51
 */
public class WordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout",new SentenceSpout());
        builder.setBolt("split-bolt",new SplitSentenceBolt()).shuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt",new WordCountBolt()).shuffleGrouping("split-bolt");
        builder.setBolt("count-bolt",new WordCountBolt()).shuffleGrouping("split-bolt");



    }
}
