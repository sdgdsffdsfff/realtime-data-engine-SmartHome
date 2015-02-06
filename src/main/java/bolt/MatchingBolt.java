package bolt;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created��4 Jan 2015 11:08:18 
 */

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import util.SystemConfig;
import Trigger.EmbededTriggerMap;
import Trigger.RunTimeTrigger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cooxm.devicecontrol.device.*;

public class MatchingBolt  implements IRichBolt {
	
	OutputCollector _collector;
	private static EmbededTriggerMap embededTriggerMap=null;
	
	public static void main(String[] args) {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		TriggerMap triggerMap = new TriggerMap(config.getMysql());
		embededTriggerMap=new EmbededTriggerMap(triggerMap);		
		/*Config conf=new Config();
		String mysql_ip			=conf.getValue("mysql_ip");
		String mysql_port		=conf.getValue("mysql_port");
		String mysql_user		=conf.getValue("mysql_user");
		String mysql_password	=conf.getValue("mysql_password");
		String mysql_database	=conf.getValue("mysql_database");
		MySqlClass mysql=new MySqlClass(mysql_ip, mysql_port, mysql_database, mysql_user, mysql_password);
		triggerMap=new TriggerMap(mysql);*/
	}

	@Override
	public void execute(Tuple input) {
		if(input==null){
			return;
		}
		RunTimeTrigger matchedTrigger=null;
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int ctrolID=(Integer)line.get(fields.indexOf("ctrolID"));
		Map<Integer, RunTimeTrigger>  triggerList=embededTriggerMap.get(ctrolID);
		for (Entry<Integer, RunTimeTrigger>  entry:triggerList.entrySet()) {			
			matchedTrigger=entry.getValue().dataMatching(line, fields);
			if(matchedTrigger!=null){
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
