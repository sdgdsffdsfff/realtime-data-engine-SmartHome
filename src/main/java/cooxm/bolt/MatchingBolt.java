package cooxm.bolt;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cooxm.devicecontrol.device.*;
import cooxm.trigger.RunTimeTrigger;
import cooxm.trigger.RuntimeTriggerMap;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created 4 Jan 2015 11:08:18 
 * 这个Bolt匹配方法是 用户在手机端个性化设置规则，云端为每一个用户保存个性化的规则。 云端用书局去匹配每一个规则。
 */
public class MatchingBolt  implements IRichBolt {
	
	OutputCollector _collector;
	private static RuntimeTriggerMap runtimeTriggerMap=null;
	//Jedis jedis;
	
	


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		TriggerMap triggerMap = new TriggerMap(config.getMysql());
		runtimeTriggerMap=new RuntimeTriggerMap(triggerMap);	
		//this.jedis=config.getJedis(); this.jedis.select(9);
	}

	@Override
	public void execute(Tuple input) {
		if(input==null ){
			return;
		}
		RunTimeTrigger matchedTrigger=null;
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int ctrolID=Integer.parseInt((String) line.get(fields.indexOf("ctrolID")));
		Map<Integer, RunTimeTrigger>  triggerList=runtimeTriggerMap.get(ctrolID);
		if(triggerList==null){
			return;
		}
		for (Entry<Integer, RunTimeTrigger>  entry:triggerList.entrySet()) {			
			matchedTrigger=entry.getValue().dataMatching(line, fields);
			if(matchedTrigger!=null){
				System.out.println("\t---------  matched: ctrolID="+matchedTrigger.getCtrolID()+",TriggerID"+matchedTrigger.getTriggerID());
				_collector.emit(new Values(matchedTrigger.getCtrolID(),matchedTrigger));				
			}
			
			/*if(null!=matchedFactors){
				for (int i = 0; i < matchedFactors.size(); i++) {
					if(matchedFactors.get(i)!=null){
						String[] values=matchedFactors.get(i).split("_");
						_collector.emit(new Values(values));
					}					
				}
			}*/
		}		
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ctrolID","trigger"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
