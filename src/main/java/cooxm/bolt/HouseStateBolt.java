package cooxm.bolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import cooxm.devicecontrol.control.Configure;
import cooxm.state.HouseState;
import cooxm.state.HouseStateMap;
import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 7, 2015 2:32:55 PM 
 */

public class HouseStateBolt implements IRichBolt {
	
	static Logger log =Logger.getLogger(HouseStateBolt.class);
	Jedis jedis=null;
	OutputCollector _collector;
	static HouseStateMap houseStateMap;//=new HouseStateMap();
	private static final int FACTOR_VALUE_LIST_SIZE=10;


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		this.jedis=new Jedis(redis_ip, redis_port,200);	
		jedis.select(9);
		this.houseStateMap=new HouseStateMap();		
		Thread timerThread=new Thread(new TimerThread(jedis));
		timerThread.setName("timerThread");
		timerThread.start();
	}

	@Override
	public void execute(Tuple input) {
		if(input==null ){
			return;
		}
		//log.info(input.getValues().toString());
		List<Object> line=input.getValues();
		List<String> fields=input.getFields().toList();
		int ctrolID=Integer.parseInt((String) line.get(fields.indexOf("ctrolID")));
		int factorID=Integer.parseInt((String) line.get(fields.indexOf("factorID")));	
		int deviceID=Integer.parseInt((String) line.get(fields.indexOf("deviceID")));	
		int roomID  =Integer.parseInt((String) line.get(fields.indexOf("roomID")));
		int value=Integer.parseInt((String) line.get(fields.indexOf("value")));
		long date = Long.parseLong( (String)line.get(fields.indexOf("timeStamp")))/1000;
		
		jedis.hset("sensorData:"+ctrolID, deviceID+"", date+"");
		
		
		HouseState houseState = houseStateMap.stateMap.get(ctrolID);
		if(houseState==null){
			houseState=new HouseState();
			houseState.setCtrolID(ctrolID);
		}
		HashMap<Integer, ArrayBlockingQueue<Integer>> factorStateMap= houseState.get(roomID);
		ArrayBlockingQueue<Integer> valueList;
		if(factorStateMap==null){
			factorStateMap=new HashMap<Integer, ArrayBlockingQueue<Integer>>();
			valueList=new ArrayBlockingQueue<Integer>(FACTOR_VALUE_LIST_SIZE);
		}else {
			valueList=factorStateMap.get(factorID);
			if(valueList==null){
				valueList=new ArrayBlockingQueue<Integer>(FACTOR_VALUE_LIST_SIZE);
			}
		}
		if(valueList.size() >= FACTOR_VALUE_LIST_SIZE){ 
			valueList.poll();  
        }  
		valueList.offer(value);
		factorStateMap.put(factorID, valueList);
		houseState.put(roomID, factorStateMap);	
		houseStateMap.stateMap.put(ctrolID, houseState);
		
		//剩下的工作由TimerThread完成：timerThread定时将houseState保存到redis
		
		this._collector.emit(new Values("houseState"));

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
