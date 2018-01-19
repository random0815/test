package storm.myUV;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by shuangmm on 2018/1/12
 */
public class UvSum extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Set<String>> counts = new HashMap<String, Set<String>>();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {

        String url = input.getStringByField("url");
        String sid = input.getStringByField("sid");

        Set<String> sids= counts.get(url);
        if (sids==null){
            sids=new HashSet<String>();
        }
        sids.add(sid);
        counts.put(url,sids);
        System.out.println("网址    :       访问人数");
        for (String key:counts.keySet()
                ) {
            System.out.println(key+":"+counts.get(key).size());
        }
        System.out.println("============");
    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
