package cooxm.state;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import cooxm.bolt.HouseStateBolt;
import redis.clients.jedis.Jedis;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jul 1, 2015 7:22:18 PM 
 */

public class TimerThread implements Runnable{

	Jedis jedis;
	public TimerThread(Jedis jedis ){
		this.jedis=jedis;
		jedis.select(9);
	}


	@Override
	public void run() {
	    while(true){
			try {
				Thread.sleep(60*1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			DateFormat sdf=new SimpleDateFormat("HH:mm:ss");
			Date nowDate=new Date();
			HouseStateBolt.houseStateMap.getAverageHouseState();
			for (Map.Entry<Integer, HashMap<Integer,  String>> entry1 : HouseStateBolt.houseStateMap.avgStateMap.entrySet()) {		
				for (Map.Entry<Integer,  String> entry2: entry1.getValue().entrySet()) {
					System.out.println(sdf.format(nowDate)+" | "+String.valueOf(entry1.getKey())+" | "+String.valueOf(entry2.getKey())+" | "+ entry2.getValue());					
					jedis.hset("houseState:"+entry1.getKey(),entry2.getKey()+"" ,  entry2.getValue());
					jedis.publish("stateUpdate", "houseState："+entry1.getKey());					
				}				
			}
			
	    }

	}
	
	public static void main(String[] args) {

	}

}
