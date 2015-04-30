package bolt;


import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import trigger.EmbededTriggerMap;
import trigger.RunTimeTrigger;
import trigger.RunTimeTriggerTemplate;
import util.SystemConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cooxm.devicecontrol.device.*;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created 4 Jan 2015 11:08:18 
 * 这个Bolt匹配方法是 ：云端认为规则对每个用户都式样的。
 */
public class MatchingBolt2  implements IRichBolt {
	
	OutputCollector _collector;
	public  static TriggerTemplateMap triggerMap=null;
	//Jedis jedis;
	
	


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		triggerMap = new TriggerTemplateMap(config.getMysql());
//		for (Entry<Integer, TriggerTemplate>  entry:triggerMap.entrySet()) {
//			entry.getValue().print();
//		}

	}

	@Override
	public void execute(Tuple input) {
		if(input==null ){
			return;
		}
		int  matchedTriggerID;
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int ctrolID=Integer.parseInt((String) line.get(fields.indexOf("ctrolID")));
		int roomID=Integer.parseInt( (String) line.get(fields.indexOf("roomID")));
		for (Entry<Integer, TriggerTemplate>  entry:triggerMap.entrySet()) {	
			RunTimeTriggerTemplate runTrigger=new RunTimeTriggerTemplate(entry.getValue(),new Date(0),0, SystemConfig.getConf().getTriggerTimeOut());
			matchedTriggerID=runTrigger.dataMatching(line, fields);
			if(matchedTriggerID!=-1){
				//System.out.println("\n \t\t---------  matched: ctrolID="+ctrolID+",TriggerID"+matchedTriggerID);
				_collector.emit(new Values(ctrolID,roomID,matchedTriggerID));				
			}
		}		
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ctrolID","roomID","trigger"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
