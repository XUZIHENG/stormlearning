/**
 * Created by Administrator on 2017/5/27.
 */

import Bolt.ReportBolt;
import Bolt.SplitSentenceBolt;
import Bolt.WordCountBolt;
import Spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * @date 2017/5/27
 * @time 16:51
 */
public class WordCountTopology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout",new SentenceSpout());
        builder.setBolt("split-bolt",new SplitSentenceBolt()).shuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt",new WordCountBolt()).fieldsGrouping("split-bolt",new Fields("word"));
        builder.setBolt("report-bolt",new ReportBolt()).globalGrouping("count-bolt");
        Config config = new Config();
        //local
//        config.put(Config.TOPOLOGY_DEBUG, false);
//        config.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount",config,builder.createTopology());
        TimeUnit.SECONDS.sleep(10);
        cluster.killTopology("WordCount");
        cluster.shutdown();

    }
}
