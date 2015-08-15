package cooxm.bolt;


import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import jline.internal.Log;
import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cooxm.devicecontrol.control.ConnectThread;
import cooxm.devicecontrol.device.*;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.trigger.RunTimeTrigger;
import cooxm.trigger.RunTimeTriggerTemplate;
import cooxm.trigger.RuntimeTriggerTemplateMap;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created 4 Jan 2015 11:08:18 
 * 这个Bolt匹配方法是 ：云端认为规则对每个用户都式样的。
 */
public class MatchingBolt2  implements IRichBolt {
	static Logger log= Logger.getLogger(MatchingBolt2.class);
	
	OutputCollector _collector;
	public  static TriggerTemplateMap triggerMap=null;  //模板
	public  static RuntimeTriggerTemplateMap runTriggerMap=null; //运行时模板
	Jedis jedis;

	
	


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		MySqlClass mysql=config.getMysql();
		//TriggerTemplateMap triggerTempMap=new TriggerTemplateMap(mysql);
		triggerMap = new TriggerTemplateMap(mysql);
		this.runTriggerMap=new RuntimeTriggerTemplateMap(triggerMap,mysql);	
		
		this.jedis=config.getJedis();
		this.jedis.select(9);
	}

	@Override
	public void execute(Tuple input) {
		if(input==null ){
			return;
		}
		//System.out.println(input.getValues());
		int  matchedTriggerID;
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int factorID=Integer.parseInt((String) line.get(fields.indexOf("factorID")));
		int ctrolID=Integer.parseInt((String) line.get(fields.indexOf("ctrolID")));
		int roomID=Integer.parseInt( (String) line.get(fields.indexOf("roomID")));
		TriggerTemplateMap triggertemp=(TriggerTemplateMap) this.runTriggerMap.get(ctrolID+"_"+roomID);
		if(triggertemp==null){
			this.runTriggerMap.put(ctrolID+"_"+roomID, (TriggerTemplateMap) this.triggerMap.clone());
			triggertemp=(TriggerTemplateMap) triggerMap.clone();
		}
		for (Entry<Integer, TriggerTemplate>  entry:triggertemp.entrySet()) {	
			RunTimeTriggerTemplate runTrigger=null;//=new RunTimeTriggerTemplate(entry.getValue(),new Date(0),0, SystemConfig.getConf().getTriggerTimeOut());
			String className=entry.getValue().getClass().getName();
			if (className.equals("cooxm.trigger.RunTimeTriggerTemplate")) {
				 runTrigger=(RunTimeTriggerTemplate) entry.getValue();
			}else {
				runTrigger=new RunTimeTriggerTemplate(entry.getValue(),new Date(0),0, SystemConfig.getConf().getTriggerTimeOut());
			}
			runTrigger.getState();
			matchedTriggerID=runTrigger.dataMatching(line, fields,jedis);
			if(matchedTriggerID!=-1){
				//System.out.println("\n \t\t---------  matched: ctrolID="+ctrolID+",TriggerID"+matchedTriggerID);
				_collector.emit(new Values(ctrolID,roomID,matchedTriggerID));
				log.info("MatingBolt: trigger rule has been triggerd,ctrolID="+ctrolID+",roomID="+roomID+",TriggerID="
				+matchedTriggerID+",name:"+runTrigger.getTriggerName());
				
				triggertemp.replace(runTrigger.getTriggerTemplateID(), runTrigger);
				this.runTriggerMap.put(ctrolID+"_"+roomID, triggertemp);				
			}
			if(runTrigger.getTriggerTemplateID()==120){
				triggertemp.replace(runTrigger.getTriggerTemplateID(), runTrigger);
				this.runTriggerMap.put(ctrolID+"_"+roomID, triggertemp);
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
