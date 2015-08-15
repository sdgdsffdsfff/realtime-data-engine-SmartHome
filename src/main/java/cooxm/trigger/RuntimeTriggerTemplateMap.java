package cooxm.trigger;

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
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.TriggerTemplateFactor;
import cooxm.devicecontrol.device.TriggerTemplateMap;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：May 6, 2015 10:27:06 AM 
 */


/**<ctrolID,TriggerTemplateMap> 用户triggerMap */
public class RuntimeTriggerTemplateMap extends HashMap<String, TriggerTemplateMap> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static Logger log= Logger.getLogger(RuntimeTriggerTemplateMap.class);
	private static final int boundary= 3000;
	
	public RuntimeTriggerTemplateMap(){}
	
	public RuntimeTriggerTemplateMap(TriggerTemplateMap triggerMap,/*Jedis jedis,*/MySqlClass mysql){
		String sql="select distinct ctr_id,roomid from info_user_room";
		String res = mysql.select(sql);
		String[] ctrolIDs=res.split("\n");
		for (int i = 0; i < ctrolIDs.length; i++) {
			String[] cells=ctrolIDs[i].split(",");
			int ctrolID=Integer.parseInt(cells[0]);
			int roomID =Integer.parseInt(cells[1]);
			this.put(ctrolID+"_"+roomID, triggerMap );//(TriggerTemplateMap) triggerMap.clone());	
		}
	}	
	

	
	public static void main(String[] args){
		SystemConfig config= SystemConfig.getConf();
		TriggerTemplateMap triggerMap = new TriggerTemplateMap(config.getMysql());
		RuntimeTriggerTemplateMap r=new RuntimeTriggerTemplateMap(triggerMap,config.getMysql());
		System.out.println(r.size());
	}

}
