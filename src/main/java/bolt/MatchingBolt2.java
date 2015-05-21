package bolt;


import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jline.internal.Log;
import redis.clients.jedis.Jedis;
import trigger.RunTimeTrigger;
import trigger.RunTimeTriggerTemplate;
import trigger.RuntimeTriggerTemplateMap;
import util.SystemConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cooxm.devicecontrol.device.*;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created 4 Jan 2015 11:08:18 
 * 这个Bolt匹配方法是 ：云端认为规则对每个用户都式样的。
 */
public class MatchingBolt2  implements IRichBolt {
	
	OutputCollector _collector;
	public  static TriggerTemplateMap triggerMap=null;
	public  RuntimeTriggerTemplateMap runTriggerMap=null;
	Jedis jedis;
	
	


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		MySqlClass mysql=config.getMysql();
		TriggerTemplateMap triggerTempMap=new TriggerTemplateMap(mysql);
		this.runTriggerMap=new RuntimeTriggerTemplateMap(triggerTempMap,mysql);
		triggerMap = new TriggerTemplateMap(config.getMysql());
		
		this.jedis=config.getJedis();


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
		TriggerTemplateMap triggertemp=this.runTriggerMap.get(ctrolID);
		if(triggertemp==null){
			//Log.warn("can't find trigger from RuntimeTriggerTemplateMap by ctrolID:"+ctrolID+",please check table info_user_room_st.");
			this.runTriggerMap.put(ctrolID, this.triggerMap);
			triggertemp=triggerMap;
		}
		for (Entry<Integer, TriggerTemplate>  entry:triggertemp.entrySet()) {	
			RunTimeTriggerTemplate runTrigger=new RunTimeTriggerTemplate(entry.getValue(),new Date(0),0, SystemConfig.getConf().getTriggerTimeOut());
			matchedTriggerID=runTrigger.dataMatching(line, fields,jedis);
			if(matchedTriggerID!=-1){
				//System.out.println("\n \t\t---------  matched: ctrolID="+ctrolID+",TriggerID"+matchedTriggerID);
				_collector.emit(new Values(ctrolID,roomID,matchedTriggerID));
				System.out.println("MatingBolt:Congrats!! rule has been triggerd,ctrolID="+ctrolID+",roomID="+roomID+",TriggerID="+matchedTriggerID);				
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
