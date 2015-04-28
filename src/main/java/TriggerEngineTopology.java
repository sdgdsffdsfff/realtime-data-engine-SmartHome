import spout.FileSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.HouseStateBolt;
import bolt.HouseStateBolt;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Feb 6, 2015 4:54:56 PM 
 */

public class TriggerEngineTopology {
	

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    //builder.setSpout("spout", new SocketSpout(), 2);
    builder.setSpout("spout", new FileSpout(), 1);
    builder.setBolt("getState",new HouseStateBolt(),2).fieldsGrouping("spout", new Fields("ctrolID"));
    //builder.setBolt("match", new MatchingBolt(),  4).fieldsGrouping("spout", new Fields("ctrolID"));
    //builder.setBolt("trigger", new ReactBolt(), 1).fieldsGrouping("match", new Fields("ctrolID"));


    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(5);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(12);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("trigger-engine", conf, builder.createTopology());
      //Thread.sleep(1000000);
      //cluster.shutdown();
    }
  }

}
