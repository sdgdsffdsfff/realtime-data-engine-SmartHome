package state;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.Configure;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 13, 2015 11:02:37 AM 
 */

public class HouseStateMap {
	
	public HouseStateMap(){
		stateMap = new HashMap<Integer, HouseState>();
		avgStateMap = new HashMap<Integer, HashMap<Integer, Double>>();
	}
	
	/** ctrolID, houseState */
	public  Map<Integer, HouseState> stateMap ;//= new HashMap<Integer, HouseState>();
	/** ctrolID, Map< Integer, Double > */
	public  Map<Integer,HashMap<Integer, Double>> avgStateMap;// = new HashMap<Integer, HashMap<Integer, Double>>();
	
	public String getAverageHouseState(){		
		String averageHouseStateString=null;
		for (Map.Entry<Integer, HouseState> entry1 : stateMap.entrySet()) {
			HashMap<Integer, Double> factorAvgMap=new HashMap<Integer, Double>();
			for (HashMap.Entry<Integer, HashMap<Integer, Integer>> entry2 : entry1.getValue().entrySet()) {
				int sum=0;
				int count=entry2.getValue().size();
				double factorAvg=-1;
				if(count>=1){
					for (Map.Entry<Integer, Integer> entry3 : entry2.getValue().entrySet()){
						sum+=entry3.getValue();					
					}
					factorAvg=sum*1.0/(count);
				}else{
					factorAvg=avgStateMap.get(entry1.getKey()).get(entry2.getKey());
				}
				factorAvgMap.put(entry2.getKey(), factorAvg);
			}	
			avgStateMap.put(entry1.getKey(), factorAvgMap);
			
			averageHouseStateString=getAverageHouseStateString(factorAvgMap);
		}
		return averageHouseStateString;
	}
	
	public String getAverageHouseStateString(HashMap<Integer, Double> factorAvgMap){
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
		for  (HashMap.Entry<Integer, Double> entry: factorAvgMap.entrySet()){
			switch (entry.getKey()) {
			//2015-04-11 richard 重新定义
			case 2001:
				avgArray[0]=entry.getValue();
				break;
			case 2011:
				avgArray[1]=entry.getValue();
				break;
			case 2021:
				avgArray[2]=entry.getValue();
				break;
			case 2031:
				avgArray[3]=entry.getValue();
				break;
			case 2041:
				avgArray[4]=entry.getValue();
				break;
			case 2061:
				avgArray[5]=entry.getValue();
				break;
			case 2301:
				avgArray[6]=entry.getValue();
				break;
			case 1201:
				avgArray[7]=entry.getValue();
				break;
			case 1301:
				avgArray[8]=entry.getValue();
				break;
			//------------------- 2015-04-11 之前 factorDict旧的 定义	
			case 201:  //光
				avgArray[0]=entry.getValue();
				break;
			case 301:  //PM2.5
				avgArray[1]=entry.getValue();
				break;
			case 401:  //有害气体-空气质量
				avgArray[6]=entry.getValue();
				break;
			case 501:   //湿度
				avgArray[3]=entry.getValue();
				break;
			case 601:   //温度
				avgArray[4]=entry.getValue();
				break;
			case 801:  // 人体探测
				avgArray[2]=entry.getValue();
				break;
			case 901:   // 噪音
				avgArray[5]=entry.getValue();
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
	

	public static void main(String[] args) {

		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		System.out.println(redis_ip+" "+redis_port);
		Jedis jedis=new Jedis(redis_ip, redis_port,500);	
		//jedis.hset("houseState", String.valueOf(123456), "1.5,2.5,3.5");
		System.out.println(jedis.hget("houseState", "123456"));
	}

}
