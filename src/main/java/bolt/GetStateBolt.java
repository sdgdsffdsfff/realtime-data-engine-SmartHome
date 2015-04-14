package bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cooxm.devicecontrol.control.Configure;
import redis.clients.jedis.Jedis;
import state.HouseState;
import state.HouseStateMap;
import trigger.RunTimeTrigger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 7, 2015 2:32:55 PM 
 */

public class GetStateBolt implements IRichBolt {
	
	Jedis jedis=null;
	OutputCollector _collector;
	HouseStateMap houseStateMap;


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		this.jedis=new Jedis(redis_ip, redis_port,200);		
		this.houseStateMap=new HouseStateMap();
	}

	@Override
	public void execute(Tuple input) {
		if(input==null ){
			return;
		}
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int ctrolID=Integer.parseInt((String) line.get(fields.indexOf("ctrolID")));
		int factorID=Integer.parseInt((String) line.get(fields.indexOf("factorID")));	
		int deviceID=Integer.parseInt((String) line.get(fields.indexOf("deviceID")));	
		int value=Integer.parseInt((String) line.get(fields.indexOf("value")));
		HouseState houseState = houseStateMap.stateMap.get(ctrolID);
		if(houseState==null){
			houseState=new HouseState();
			houseState.setCtrolID(ctrolID);
		}
		HashMap<Integer, Integer> factorStateMap= houseState.get(ctrolID);
		if(factorStateMap==null){
			factorStateMap=new HashMap<Integer, Integer>();
		}
		factorStateMap.put(deviceID, value);
		houseState.put(factorID, factorStateMap);	
		houseStateMap.stateMap.put(ctrolID, houseState);
		

		//int second=java.util.Calendar.SECOND;
		Date nowDate=new Date();
		int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		if( /*(min%2) ==0 && */(second==0) ){
		System.out.println(second +" | "+ String.valueOf(ctrolID)+" | "+ houseStateMap.getAverageHouseState());
		//if( (second%10==0) ){	
			jedis.hset("houseState", String.valueOf(ctrolID), houseStateMap.getAverageHouseState());
			this.houseStateMap=new HouseStateMap();
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


	
}
