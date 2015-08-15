package cooxm.bolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;
import org.json.JSONException;

import cooxm.devicecontrol.control.Configure;
import cooxm.state.HouseState;
import cooxm.state.HouseStateMap;
import cooxm.state.TimerThread;
import cooxm.util.RedisUtil;
import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
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
	
	public static HouseStateMap houseStateMap;
	private static final int FACTOR_VALUE_LIST_SIZE=10;


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		this.jedis=RedisUtil.getJedis();//new Jedis(redis_ip, redis_port,10000);	
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
		Double value=Double.parseDouble((String) line.get(fields.indexOf("value")));
		
		
		double deltaValue=0.0;
		List<Object> em = null;
		if(factorID==2501 ||factorID==2506 /*||factorID==2502 ||factorID==2504 ||factorID==2505  ||factorID==2507*/ ){
			try {
				double avgValue=houseStateMap.getRoomFactorAvg(ctrolID, roomID, factorID);
				if(avgValue!=-65535){
					deltaValue=value-avgValue;
					 em= input.getValues();
					 em.set(0, factorID+100+"");
					 em.set(7, deltaValue);
					 this._collector.emit(em);
					 //for (Object st:em) {System.out.print(st+",");}System.out.print("\n");
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	
		
		HouseState houseState = houseStateMap.stateMap.get(ctrolID);
		if(houseState==null){
			houseState=new HouseState();
			houseState.setCtrolID(ctrolID);
		}
		ArrayBlockingQueue<Double> valueList;
		if(factorID==2502){
			//对每个房间都添加 PM25数据
			for (Entry<Integer, HashMap<Integer, ArrayBlockingQueue<Double>>> entry1 :houseState.entrySet()) {  //<roomID,<factorID,ArrayBlockingQueue<Double>>>
				if (entry1.getValue().containsKey(2502)) {
					valueList=entry1.getValue().get(2502);					
				}else{
					valueList=new ArrayBlockingQueue<Double>(FACTOR_VALUE_LIST_SIZE);
				}
				if(valueList.size() >= FACTOR_VALUE_LIST_SIZE){ 
					valueList.poll();  
		        }
				valueList.add(value);
				entry1.getValue().put(2502, valueList);	
				houseState.put(entry1.getKey(), entry1.getValue());  //<roomID,factorMap>
			}
			houseStateMap.stateMap.put(ctrolID, houseState);
			return;
		}
		HashMap<Integer, ArrayBlockingQueue<Double>> factorStateMap= houseState.get(roomID);

		if(factorStateMap==null){
			factorStateMap=new HashMap<Integer, ArrayBlockingQueue<Double>>();
			valueList=new ArrayBlockingQueue<Double>(FACTOR_VALUE_LIST_SIZE);
		}else {
			valueList=factorStateMap.get(factorID);
			if(valueList==null){
				valueList=new ArrayBlockingQueue<Double>(FACTOR_VALUE_LIST_SIZE);
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

		

	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("factorID","timeStamp","ctrolID","deviceID","roomType","roomID","wallID","value","rate"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


	
}
