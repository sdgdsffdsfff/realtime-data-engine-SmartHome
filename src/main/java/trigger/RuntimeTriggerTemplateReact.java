package trigger;

import java.sql.SQLException;
import java.util.Date;
import java.util.Set;

import jline.internal.Log;

import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.Device;
import cooxm.devicecontrol.device.DeviceState;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.ProfileSet;
import cooxm.devicecontrol.device.TriggerTemplateReact;
import cooxm.devicecontrol.device.Warn;
import cooxm.devicecontrol.socket.Message;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：May 8, 2015 3:16:31 PM 
 */

public class RuntimeTriggerTemplateReact extends TriggerTemplateReact {
	static long cookieNo=((System.currentTimeMillis()/1000)%(24*3600))*10000;
	
	public RuntimeTriggerTemplateReact(){		
	}
	
	public RuntimeTriggerTemplateReact(TriggerTemplateReact react){	
		super(react);
		this.cookieNo ++;		
	}
	
	public Message react(MySqlClass mysql,Jedis jedis,int ctrolID,int roomID) {
		int onOFF=-1;
		int mode=-1;
		int tempreture=-1;
		int speed=-1;
		int channel=-1;
		int volumn=-1;

		String cookie=this.cookieNo+"_5";
		//this.cookieNo++;
		Message msg=null;
		JSONObject json;
		switch (getReactType()) {
		case 1:  //通知或告警
			Warn warn=new Warn(ctrolID, 3, 3, 0, new Date(), 2, this.getTargetID(), 2);
			json=new JSONObject();
			try {
				json.put("ctrolID", ctrolID);
				json.put("sender",5);
				json.put("receiver",0); 
				json.put("warn", warn.toJsonObject());
			} catch (JSONException e2) {
				e2.printStackTrace();
			}

			msg=new Message((short) (LogicControl.WARNING_START+3), cookie,json );
			break;
		case 2:  //家电			
			switch (getReactWay()) {
			case 11:    //		11：打开
				 onOFF=0;
				 break;
			case 12:   	//		12：关闭
				 onOFF=1;
				 break;
			case 1300:  // 打开且 自动模式
				onOFF=0; 
				mode=0;
				 break;
			case 1301:  // 打开且制冷
				onOFF=0;
				mode=1;
				 break;
			case 1302:  // 打开且除湿模式
				onOFF=0;
				mode=2;	
				 break;
			case 1303:  // 打开且智能模式
				onOFF=0;
				mode=3;	
				 break;
			case 1304:  // 打开且制热模式
				onOFF=0;
				mode=4;	
				 break;
			case 1400:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=16;
				 break;
			case 1401:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=17;
				 break;
			case 1402:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=18;
			case 1403:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=19;
				 break;
			case 1404:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=20;
				 break;
			case 1405:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=21;
				 break;
			case 1406:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=22;
				 break;
			case 1407:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=23;
				 break;
			case 1408:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=24;
				 break;
			case 1409:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=25;
				 break;
			case 1410:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=26;
				 break;
			case 1411:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=27;
				 break;
			case 1412:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=28;
				 break;
			case 1413:  // 打开且温度设置为16度
				onOFF=0;
				tempreture=29;
				 break;
			case 1414:  // 打开且温度设置为30度
				onOFF=0;
				tempreture=30;
			case 1500:  // 打开且风速设置为自动
				onOFF=0;
				tempreture=30;
				 break;
			case 1501:  // 打开且风速设置为1
				onOFF=0;
				tempreture=30;
				 break;
			case 1502:  // 打开且风速设置为2
				onOFF=0;
				tempreture=30;
				 break;
			case 1503:  // 打开且风速设置为3
				onOFF=0;
				tempreture=30;
				 break;
			default:
				break;
			}
			DeviceState state= new DeviceState(onOFF, mode, speed, -1, tempreture, channel, volumn, -1);
			Set<String> deviceIDSet = jedis.hkeys(ctrolID+"_roomBind");
			if(deviceIDSet.size()==0){
				Log.error("can't find roomBind table in redis,ctrolID="+ctrolID+",DeviceType="+this.getTargetID()+",roomID="+roomID);
			}
			for (String deiceID:deviceIDSet) {
				Device device=new Device();
				String s=jedis.hget(ctrolID+"_roomBind", deiceID);
				//Log.warn(s);
				try {
					device = new Device(new JSONObject(s));
				} catch (JSONException e1) {
					e1.printStackTrace();
				}
				if (device.getDeviceType()==this.getTargetID()) {
					json=new JSONObject();
					try {
						json.put("ctrolID", ctrolID);
						json.put("roomID", roomID);
						json.put("deviceID", device.getDeviceID());
						json.put("deviceType", device.getDeviceType());
						json.put("sender",5);
						json.put("receiver",0); 
						json.put("state", state.toJson());
					} catch (JSONException e) {
						e.printStackTrace();
					}					
					msg=new Message((short) (LogicControl.SWITCH_DEVICE_STATE), cookie,json );	
				}else{
					continue;
				}	
			}			
			
			break;
		case 3:  //profile
			Profile p;
			try {
				p = Profile.getFromDBByTemplateID(mysql, ctrolID, roomID,getTargetID()); // targertID就是情景模板ID
				json=new JSONObject();
				json.put("ctrolID", ctrolID);
				json.put("sender",5);
				json.put("receiver",0); 
				json.put("profileID",p.getProfileID()); 
				msg=new Message((short) (LogicControl.SWITCH_RROFILE_SET), cookie,json);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (JSONException e) {   
				e.printStackTrace();
			}	
			break;
		case 4:  //profileSet
			ProfileSet ps;
			try {
				ps = ProfileSet.getProfileSetByTemplateID(mysql, ctrolID, getTargetID()); // targertID就是情景模板ID
				json=new JSONObject();
				json.put("ctrolID", ctrolID);
				json.put("sender",5);
				json.put("receiver",0); 
				json.put("profileSetID",ps.getProfileSetID()); 
				msg=new Message((short) (LogicControl.SWITCH_RROFILE_SET), cookie,json);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (JSONException e) {   
				e.printStackTrace();
			}	
			break;
		default:
			break;
		}	
		System.out.println(msg.toString());
			return msg;			
		}
	

	public static void main(String[] args) {
	   long cookieNo=((System.currentTimeMillis()/1000)%(24*3600))*10000;
	   System.out.println(cookieNo);
	}

}
