package trigger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import util.SystemConfig;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.TriggerTemplateFactor;
import cooxm.devicecontrol.device.TriggerTemplateMap;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：May 6, 2015 10:27:06 AM 
 */

public class RuntimeTriggerTemplateMap extends HashMap<Integer, TriggerTemplateMap> {
	public static Logger log= Logger.getLogger(RuntimeTriggerTemplateMap.class);
	private static final int boundary= 3000;
	
	public RuntimeTriggerTemplateMap(){}
	
	public RuntimeTriggerTemplateMap(TriggerTemplateMap triggerMap,/*Jedis jedis,*/MySqlClass mysql){
		String sql="select ctr_id from info_user_room_st";
		String res = mysql.select(sql);
		String[] ctrolIDs=res.split("\n");
		//Set<String> ctrolIDs = jedis.keys("*_currentProfile");
		//for (String ID:ctrolIDs) {
		for (int i = 0; i < ctrolIDs.length; i++) {
			int ctrolID=Integer.parseInt(ctrolIDs[i]);
			this.put(ctrolID, triggerMap);	
		}
	}	
	

	
	public static void main(String[] args){
		SystemConfig config= SystemConfig.getConf();
		TriggerTemplateMap triggerMap = new TriggerTemplateMap(config.getMysql());
		RuntimeTriggerTemplateMap r=new RuntimeTriggerTemplateMap(triggerMap,config.getMysql());
		System.out.println(r.size());
	}

}
