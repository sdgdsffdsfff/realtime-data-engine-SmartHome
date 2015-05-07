package state;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.device.Factor;
import cooxm.devicecontrol.device.FactorTemplate;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.ProfileTemplate;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 13, 2015 11:02:37 AM 
 */

public class HouseStateMap {
	

	
	/** ctrolID, houseState */
	public  Map<Integer, HouseState> stateMap ;//= new HashMap<Integer, HouseState>();
	/** ctrolID, Map< Integer, Double > */
	public  Map<Integer,HashMap<Integer, Double>> avgStateMap;// = new HashMap<Integer, HashMap<Integer, Double>>();
	
	
	public Map<Integer, HouseState> getStateMap() {
		return stateMap;
	}


	public void setStateMap(Map<Integer, HouseState> stateMap) {
		this.stateMap = stateMap;
	}


	public Map<Integer, HashMap<Integer, Double>> getAvgStateMap() {
		return avgStateMap;
	}


	public void setAvgStateMap(Map<Integer, HashMap<Integer, Double>> avgStateMap) {
		this.avgStateMap = avgStateMap;
	}


	public HouseStateMap(){
		stateMap = new HashMap<Integer, HouseState>();
		avgStateMap = new HashMap<Integer, HashMap<Integer, Double>>();
	}
	
	
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
					if(entry2.getKey()==2503){
						factorAvg=Math.floor(factorAvg);
					}
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
			//------------------- 2015-04-11 之前 factorDict旧的 定义	
			case 2011:  //光
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
	

	public static void main(String[] args) throws SQLException {

		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		Jedis jedis=new Jedis(redis_ip, redis_port,500);
		
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");
		List<ProfileTemplate> ptempList=ProfileTemplate.getAllFromDB(mysql);
//		ProfileTemplate a = ptempList.get(0);
//		FactorTemplate b = ptempList.get(0).getFactorTemplateTempList().get(0);
		
		int[] ids={1256788,1256789};
		for (int i = 2; i <3; i++) {
			for (int j = 0; j < ids.length; j++) {
				Profile p=new Profile(ptempList.get(i), ids[j]);
				jedis.hset(p.getCtrolID()+"_currentProfile",p.getRoomID()+"", p.toJsonObj().toString());
			}
		} 
		System.out.println("success");
	}

}
