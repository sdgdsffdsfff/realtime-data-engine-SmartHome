package cooxm.state;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.EnviromentState;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.ProfileTemplate;
import cooxm.devicecontrol.device.State;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 13, 2015 11:02:37 AM 
 */

public class HouseStateMap {
	
	MySqlClass mysql;
	LevelMap levelMap;
	

	
	/** ctrolID, houseState */
	public  Map<Integer, HouseState> stateMap ;//= new HashMap<Integer, HouseState>();
	
	/** Map<ctrolID,Map<roomID, JsonString> >> */
	public Map<Integer, HashMap<Integer,  String>> avgStateMap;// = new HashMap<Integer, HashMap<Integer, Double>>();
	
	/**     Map<ctrolID,HashMap<roomID, HashMap<factorID,  avg>> > */
	//public  Map<Integer,HashMap<Integer,HashMap<Integer,  Double>>> roomFactorAvgMap;  
	
	
	public Map<Integer, HouseState> getStateMap() {
		return stateMap;
	}


	public void setStateMap(Map<Integer, HouseState> stateMap) {
		this.stateMap = stateMap;
	}


	public Map<Integer, HashMap<Integer, String>> getAvgStateMap() {
		return avgStateMap;
	}


	public void setAvgStateMap(Map<Integer, HashMap<Integer, String>> avgStateMap) {
		this.avgStateMap = avgStateMap;
	}


	public HouseStateMap(){
		stateMap = new HashMap<Integer, HouseState>();
		avgStateMap = new HashMap<Integer, HashMap<Integer,  String>>();
		SystemConfig confg=SystemConfig.getConf();
		this.mysql=confg.getMysql();
		this.levelMap=new LevelMap(mysql);
		
	}
	
	/** 将以deviceID为key 的 MAP取平均值 */
	/*public String getAverageHouseState(){		
		String averageHouseStateString=null;
		for (Map.Entry<Integer, HouseState> entry1 : stateMap.entrySet()) {
			HashMap<Integer, Double> roomAvgMap=new HashMap<Integer, Double>();
			for (HashMap.Entry<Integer, HashMap<Integer, Integer>> entry2 : entry1.getValue().entrySet()) {
				int sum=0;
				int count=entry2.getValue().size();
				double factorAvg=-1;
				if(count>=1){
					for (Map.Entry<Integer, Integer> entry3 : entry2.getValue().entrySet()){
						sum+=entry3.getValue();					
					}
					factorAvg=sum*1.0/(count);
					if(entry2.getKey()==2503){
						factorAvg=Math.floor(factorAvg);
					}
				}else{
					factorAvg=avgStateMap.get(entry1.getKey()).get(entry2.getKey());
				}
				roomAvgMap.put(entry2.getKey(), factorAvg);
			}	
			avgStateMap.put(entry1.getKey(), roomAvgMap);
			
			averageHouseStateString=getAverageHouseStateString(roomAvgMap);
		}
		return averageHouseStateString;
	}
	
		public String getAverageHouseStateString(HashMap<Integer, Double> roomAvgMap){
		DecimalFormat df = new DecimalFormat("#.00");
		String avgArrayString="";
		/ *数组保存依次是：
		0 2001	光强度
		1 2011	PM2.5
		2 2021	人体探测器
		3 2031	湿度
		4 2041	温度
		5 2061	声音
		6 2301	有害气体探测器-空气质量
		7 1201	烟雾探测器-一氧化碳
		8 1301	漏水探测器
		* /
		Double[] avgArray={(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1};
		for  (HashMap.Entry<Integer, Double> entry: roomAvgMap.entrySet()){
			switch (entry.getKey()) {
			//2015-05-04 richard 重新定义
			case 2501: //光
				avgArray[0]=entry.getValue();
				break;
			case 2502: //PM2.5
				avgArray[1]=entry.getValue();
				break;
			case 2503: //人体探测器
				avgArray[2]=entry.getValue();
				break;
			case 2504:  //湿度
				avgArray[3]=entry.getValue();
				break;
			case 2505:  //温度
				avgArray[4]=entry.getValue();
				break;
			case 2506:  //噪音
				avgArray[5]=entry.getValue();
				break;
			case 2507:  // 空气质量-6合1
				avgArray[6]=entry.getValue();
				break; 
			case 201:  //烟雾探测器-一氧化碳
				avgArray[7]=entry.getValue();
				break;
			case 211:  //漏水探测器
				avgArray[8]=entry.getValue();
				break;
				
			default:
				break;
			}			
		}
		
		for (int i = 0; i < avgArray.length; i++) {
			avgArrayString=avgArrayString+df.format(avgArray[i])+",";
		}
		return avgArrayString.substring(0, avgArrayString.length()-1);
	}
	
	*/
	
	/** 将以roomID为key 的value list取平均值,返回值为<roomID,stateString>*/
	public void getAverageHouseState(){		
		Map<Integer, String> stateStrMap=new HashMap<Integer, String>();
		String averageHouseStateString=null;
		for (Map.Entry<Integer, HouseState> entry1 : stateMap.entrySet()) {
			HashMap<Integer, String> roomAvgMap= new HashMap<Integer, String>();  //HashMap<roomID, StateString>
			for (HashMap.Entry<Integer, HashMap<Integer, ArrayBlockingQueue<Double>>> entry2 : entry1.getValue().entrySet()) {
				HashMap<Integer, Double> factorAvgMap=null;
				factorAvgMap=new HashMap<Integer, Double> ();
				for (Map.Entry<Integer, ArrayBlockingQueue<Double>> entry3 : entry2.getValue().entrySet()){
					int sum=0;
					int count=entry3.getValue().size();
					double factorAvg=-1;
					Object[] x=entry3.getValue().toArray();
					for (int i = 0; i < count; i++) {
						sum+=(Double)x[i];	
					}
				    factorAvg=sum*1.0/(count);
					if(entry2.getKey()==2503){
						factorAvg=entry3.getValue().peek();
					}
					BigDecimal   b   =   new   BigDecimal(factorAvg);
					BigDecimal c = b.setScale(2,  BigDecimal.ROUND_HALF_UP);
					factorAvgMap.put(entry3.getKey(), c.doubleValue());
				}	
				// 2015-06-01 UK 要求改为json格式
				//averageHouseStateString=getAverageHouseStateString(factorAvgMap);
				EnviromentState e = getAverageEnviromentState(factorAvgMap);
				if(e!=null){
					averageHouseStateString=e.toJson().toString();
					//System.out.println(averageHouseStateString  +"--------------------------------------");
					roomAvgMap.put(entry2.getKey(), averageHouseStateString);
				}
			}	
			avgStateMap.put(entry1.getKey(), roomAvgMap);	
		}
		return ;
	}
	
	public double getRoomFactorAvg(int ctrolID,int roomID,int factorID) throws JSONException{
		HashMap<Integer, String> roomAvgMap = this.avgStateMap.get(ctrolID);
		if(roomAvgMap!=null){
			String averageHouseStateString=roomAvgMap.get(roomID);
			if(averageHouseStateString!=null){
				JSONObject json=new JSONObject(averageHouseStateString);
				double value=-65535;
				switch (factorID) {
				//2015-05-04 richard 重新定义
				case 2501: //光
					value=json.getJSONObject("lux").getDouble("value");
					break;
				case 2502: //PM2.5
					value=json.getJSONObject("pm25").getDouble("value");;
					break;
				case 2504:  //湿度
					value=json.getJSONObject("moisture").getDouble("value");;
					break;
				case 2505:  //温度
					value=json.getJSONObject("temperature").getDouble("value");;
					break;
				case 2506:  //噪音 
					value=json.getJSONObject("noise").getDouble("value");;
					break;
				case 2507:  // 空气质量-6合1
					value=json.getJSONObject("harmfulGas").getDouble("value");;
					break; 
				default:
					break; 
			   }
			   return value;				
			}
		}
		return -65535;
	}
	
	public String getAverageHouseStateString(HashMap<Integer, Double> roomAvgMap){
		DecimalFormat df = new DecimalFormat("#.00");
		String avgArrayString="";
		/*数组保存依次是：
		0 2001	光强度
		1 2011	PM2.5
		2 2021	人体探测器
		3 2031	湿度
		4 2041	温度
		5 2061	声音
		6 2301	有害气体探测器-空气质量
		7 1201	烟雾探测器-一氧化碳
		8 1301	漏水探测器
		*/
		Double[] avgArray={(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1,(double) -1};
		for  (HashMap.Entry<Integer, Double> entry: roomAvgMap.entrySet()){
			switch (entry.getKey()) {
			//2015-05-04 richard 重新定义
			case 2501: //光
				avgArray[0]=entry.getValue();
				break;
			case 2502: //PM2.5
				avgArray[1]=entry.getValue();
				break;
			case 2503: //人体探测器
				avgArray[2]=entry.getValue();
				break;
			case 2504:  //湿度
				avgArray[3]=entry.getValue();
				break;
			case 2505:  //温度
				avgArray[4]=entry.getValue();
				break;
			case 2506:  //噪音
				avgArray[5]=entry.getValue();
				break;
			case 2507:  // 空气质量-6合1
				avgArray[6]=entry.getValue();
				break; 
			case 201:  //烟雾探测器-一氧化碳
				avgArray[7]=entry.getValue();
				break;
			case 211:  //漏水探测器
				avgArray[8]=entry.getValue();
				break;			
			default:
				break;
			}			
		}
		
		for (int i = 0; i < avgArray.length; i++) {
			avgArrayString=avgArrayString+df.format(avgArray[i])+",";
		}
		return avgArrayString.substring(0, avgArrayString.length()-1);
	}
	
	public EnviromentState getAverageEnviromentState(HashMap<Integer, Double> roomAvgMap){
		DecimalFormat df = new DecimalFormat("#.00");

		/*数组保存依次是：
		2501	光强度
		2502	PM2.5-中控上
		2503	人体红外探测器
		2504	湿度
		2505	温度
		2506	声音
		2507	空气质量-6合一传感器
		1201	烟雾探测器-一氧化碳
		1301	漏水探测器
		*/
		EnviromentState es=new EnviromentState();
		for  (HashMap.Entry<Integer, Double> entry: roomAvgMap.entrySet()){
			double value=entry.getValue();
			State state=new State(-1,-1);
			int level;
			switch (entry.getKey()) {
			//2015-05-04 richard 重新定义
			case 2501: //光
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);
				es.setLux(state);
				break;
			case 2502: //PM2.5
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);
				es.setPm25(state);
				break;
//			case 2503: //人体探测器
				//state=new State(-1,-1);
//				avgArray[2]=entry.getValue();
//				break;
			case 2504:  //湿度
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);              	
                es.setMoisture(state);
				break;
			case 2505:  //温度
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);
				es.setTemprature(state);
				break;
			case 2506:  //噪音 
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);
				es.setNoise(state);
				break;
			case 2507:  // 空气质量-6合1
				level =levelMap.getLevel((int)entry.getKey(), value);
				BigDecimal   b   =   new   BigDecimal(value/100.0);
				BigDecimal c = b.setScale(2,  BigDecimal.ROUND_HALF_UP);
                state=new State(c.doubleValue(),level);
				es.setHarmfulGas(state);
				break; 
//			case 201:  //烟雾探测器-一氧化碳
//				es.setPm25(state);
//				break;
//			case 211:  //漏水探测器
//				es.setPm25(state);
//				break;
			default:
				//es=null;
				break; 
		   }
			//  2015-08-01 已经在spout除以100 这里不再处理
			/*switch (entry.getKey()) {
			//2015-05-04 richard 重新定义
			case 2501: //光
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value,level);
				es.setLux(state);
				break;
			case 2502: //PM2.5
				level =levelMap.getLevel((int)entry.getKey(), value/100.0);
                state=new State(value/100.0,level);
				es.setPm25(state);
				break;
			case 2504:  //湿度
				level =levelMap.getLevel((int)entry.getKey(), value/100.0);
                state=new State(value/100.0,level);              	
                es.setMoisture(state);
				break;
			case 2505:  //温度
				level =levelMap.getLevel((int)entry.getKey(), value/100.0);
                state=new State(value/100.0,level);
				es.setTemprature(state);
				break;
			case 2506:  //噪音 
				level =levelMap.getLevel((int)entry.getKey(), value/100.0);
                state=new State(value/100.0,level);
				es.setNoise(state);
				break;
			case 2507:  // 空气质量-6合1
				level =levelMap.getLevel((int)entry.getKey(), value);
                state=new State(value/100.0,level);
				es.setHarmfulGas(state);
				break; 
			default:
				//es=null;
				break; 
		   }*/
	   }
       return es;
       
	}


	

	public static void main(String[] args) throws SQLException {

		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		Jedis jedis=new Jedis(redis_ip, redis_port,10000);
		jedis.select(9);
		
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "cooxm", "cooxm");
		Map<Integer,ProfileTemplate> ptempList=ProfileTemplate.getAllFromDB(mysql);
//		ProfileTemplate a = ptempList.get(0);
//		FactorTemplate b = ptempList.get(0).getFactorTemplateTempList().get(0);
		
		int[] ids={40008,1256789};
		for (int i = 2; i <3; i++) {
			for (int j = 0; j < ids.length; j++) {
				Profile p=new Profile(ptempList.get(i), ids[j]);
				jedis.hset(LogicControl.currentProfile+p.getCtrolID(),p.getRoomID()+"", p.toJsonObj().toString());
			}
		} 
		System.out.println("success");
	}

}
