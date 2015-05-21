package bolt;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

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

public class HouseStateBolt implements IRichBolt {
	
	Jedis jedis=null;
	OutputCollector _collector;
	HouseStateMap houseStateMap;//=new HouseStateMap();


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
		//int deviceID=Integer.parseInt((String) line.get(fields.indexOf("deviceID")));	
		int roomID  =Integer.parseInt((String) line.get(fields.indexOf("roomID")));
		int value=Integer.parseInt((String) line.get(fields.indexOf("value")));
		HouseState houseState = houseStateMap.stateMap.get(ctrolID);
		if(houseState==null){
			houseState=new HouseState();
			houseState.setCtrolID(ctrolID);
		}
		HashMap<Integer, ArrayBlockingQueue<Integer>> factorStateMap= houseState.get(roomID);
		ArrayBlockingQueue<Integer> valueList;
		if(factorStateMap==null){
			factorStateMap=new HashMap<Integer, ArrayBlockingQueue<Integer>>();
			valueList=new ArrayBlockingQueue<Integer>(10);
		}else {
			valueList=factorStateMap.get(factorID);
			if(valueList==null){
				valueList=new ArrayBlockingQueue<Integer>(10);
			}
		}
		if(valueList.size() >= 10){ 
			valueList.poll();  
        }  
		valueList.offer(value);
		factorStateMap.put(factorID, valueList);
		houseState.put(roomID, factorStateMap);	
		houseStateMap.stateMap.put(ctrolID, houseState);

		DateFormat sdf=new SimpleDateFormat("HH:mm:ss");
		Date nowDate=new Date();
		//int min=nowDate.getMinutes();
		int second=nowDate.getSeconds();
		if( (second%10==0) ){
			houseStateMap.getAverageHouseState();
			for (Map.Entry<Integer, HashMap<Integer,  String>> entry1 : houseStateMap.avgStateMap.entrySet()) {
				entry1.getKey();
				for (Map.Entry<Integer,  String> entry2: entry1.getValue().entrySet()) {
					entry2.getKey();
					System.out.println(sdf.format(nowDate)+" | "+String.valueOf(entry1.getKey())+" | "+String.valueOf(entry2.getKey())+" | "+ entry2.getValue());					
					jedis.hset(entry1.getKey()+"_houseState",entry2.getKey()+"" ,  entry2.getValue());
					jedis.publish("stateUpdate", entry1.getKey()+"_houseState");
				}				
			}			
			//this.houseStateMap=new HouseStateMap();
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
