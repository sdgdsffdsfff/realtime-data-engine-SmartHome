package com.cooxm.WordCountTest;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：8 Jan 2015 11:11:12 
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class FirstTopo {
    
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new RandomSpout());  
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout"); 
        Config conf = new Config();  
        conf.setDebug(false); 
        if (args != null && args.length > 0) {  
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
        }  
    }  
}