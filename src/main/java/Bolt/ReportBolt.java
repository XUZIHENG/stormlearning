package Bolt;/**
 * Created by Administrator on 2017/5/27.
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * @date 2017/5/27
 * @time 16:34
 */
public class ReportBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String,Long> counts = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        this.collector = outputCollector;
        this.counts = new HashMap<String,Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
    @Override
    public void cleanup(){
        System.out.println("---Final Counts---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key : keys){
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("-------------------");
    }
}
