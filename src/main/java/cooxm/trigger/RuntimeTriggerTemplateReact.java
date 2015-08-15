package cooxm.trigger;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
	
	public Message react(MySqlClass mysql,Jedis jedis,int ctrolID,int roomID) throws ParseException {
		DateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		jedis.select(9);
		int onOFF=-1;
		int mode=-1;
		int tempreture=26;
		int speed=0;
		int windDirection=0;
		int key=0;


		String cookie=this.cookieNo+"_5";
		//this.cookieNo++;
		Message msg=null;
		JSONObject json;
		int tempID;
		switch (getReactType()) {
		case 1:  //通知或告警
			int targetID=this.getTargetID();
			if (targetID>=100 && targetID<=2999) {  //家电ID
				targetID=targetID+1000;  //家电ID加1000 以避免和其他的告警ID冲突  2015-07-16 和谭哥约定
			}
			Warn warn=new Warn(ctrolID, 3, 3, 0, new Date(), 2, targetID, 2,this.getReactWay(),"");
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
			case 1:    //		11：打开
				if(getTargetID()==541){ //空调做特殊处理，空调0开，1关
					onOFF=0;					
				}else{
				    onOFF=501;        //其他家电打开为501
				}
				 break;
			case 0:   	//		12：关闭
				if(getTargetID()==541){ //空调做特殊处理，空调0开，1关
					onOFF=1;
				}else{
				    onOFF=502;      //其他家电打开为502
				}
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
			DeviceState state= new DeviceState(onOFF, mode, speed, windDirection, tempreture,key,-1);  //-1不会打破空调恒温
			Set<String> deviceIDSet = jedis.hkeys(LogicControl.roomBind+ctrolID);
			if(deviceIDSet.size()==0){
				Log.error("can't find roomBind table in redis,ctrolID="+ctrolID+",DeviceType="+this.getTargetID()+",roomID="+roomID);
				return null;
			}
			for (String deviceID:deviceIDSet) {
				Device device=new Device();
				String s=jedis.hget(LogicControl.roomBind+ctrolID, deviceID);
				if(s==null){
					Log.error("can't find device in redis roomBind table,ctrolID="+ctrolID+",DeviceType="+this.getTargetID()+",roomID="+roomID+",deviceID="+deviceID);
					return null;
				}
				try {
					device = new Device(new JSONObject(s));
				} catch (JSONException e1) {
					e1.printStackTrace();
					Log.error("json parse error for device,ctrolID="+ctrolID+",deviceID="+deviceID);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if (device.getDeviceType()==this.getTargetID() && device.getRoomID()==roomID) {
					json=new JSONObject();
					try {
						json.put("ctrolID", ctrolID);
						json.put("roomID", roomID);
						json.put("deviceID", device.getDeviceID());
						json.put("deviceType", device.getDeviceType());
						json.put("sender",5);
						json.put("receiver",0); 
						if(device.getDeviceType()==541) { //空调专用接口
							String airStateStr=jedis.hget(LogicControl.currentDeviceState+ctrolID, deviceID);
							JSONObject airStateJson;
							DeviceState oldState;
							if(airStateStr==null){   //初始状态不存在
								json.put("state", state.toJson());
							}else{								
								airStateJson=new JSONObject(airStateStr);
								//int stable=airStateJson.getInt("stable");
								int sender;
								if (airStateJson.has("sender")) {
									sender=airStateJson.getInt("sender");
								}else{
									sender=5;
								}
								int OnOff=-1;   //501打开，502关闭
								if(airStateJson.has("keyType")){
									OnOff=airStateJson.getInt("keyType");   //501打开，502关闭
								}else{
									JSONObject airState=airStateJson.getJSONObject("state");
									onOFF=(airState.optInt("onOff")==1)?502:501;          //1关闭，0 打开
								}								
								if((sender==0 ||sender==1)&& onOFF==502){      //最后一条指令是由中控或者手机发出，并且是关闭的
									return null;                              //不做任何操作
								}else{
									if(airStateStr!=null && airStateStr.contains("state")){   //state 是空调专用的 状态	
										JSONObject oldStateJson=new JSONObject(airStateStr);
										oldState=new DeviceState(oldStateJson);
										//int stable=oldStateJson.getInt("stable");
										if(state.getMode()==1){    //mode=1 制冷； 要求空调制冷，说明还不够冷，减2度
											state.setTempreature(oldState.getTempreature() - 1 );
											if(state.getTempreature()<16) {
												  state.setTempreature(16);
											}
										}else if(state.getMode()==4) {  //mode=4 制热 ； 要求空调制热，说明还不够热，加2度
											state.setTempreature(oldState.getTempreature() + 1 );
											if(state.getTempreature()>30){ 
												state.setTempreature(30);
											}
										}
										state.setStable(oldState.getStable());
										state.setWindDirection(oldState.getWindDirection());
										state.setWindSpeed(oldState.getWindSpeed());
										state.setKeyType(oldState.getKeyType());
										//oldState.replaceAdd(state);											
										json.put("state", state.toJson());	
									}else{
										json.put("keyType", onOFF);
									}
								}								 
							}
						}else{
							json.put("keyType", onOFF);
						}
						msg=new Message((short) (LogicControl.SWITCH_DEVICE_STATE), cookie,json );	
					} catch (JSONException e) {
						e.printStackTrace();
					}					
					
				}else{
					continue;
				}	
			}			
			
			break;
		case 3:  //profile
			Profile p;

			if(getTargetID()>10000){
				tempID=getTargetID()%10000;
			}else{
				tempID=getTargetID();
			}
			try {
				p = Profile.getFromDBByTemplateID(mysql, ctrolID, roomID,tempID); // targertID就是情景模板ID
				if(p!=null){
					json=new JSONObject();
					json.put("ctrolID", ctrolID);
					json.put("sender",5);
					json.put("receiver",0); 
					json.put("profileID",p.getProfileID()); 
					msg=new Message((short) (LogicControl.SWITCH_RROFILE_SET), cookie,json);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (JSONException e) {   
				e.printStackTrace();
			}	
			break;
		case 4:  //profileSet
			ProfileSet ps;
			if(getTargetID()>10000){
				tempID=getTargetID()%10000;
			}else{
				tempID=getTargetID();
			}
			try {				
				ps = ProfileSet.getProfileSetByTemplateID(mysql, ctrolID, tempID); // targertID就是情景模板ID
				if(ps!=null){
					json=new JSONObject();
					json.put("ctrolID", ctrolID);
					json.put("sender",5);
					json.put("receiver",0); 
					json.put("profileSetID",ps.getProfileSetID()); 
					msg=new Message((short) (LogicControl.SWITCH_RROFILE_SET), cookie,json);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (JSONException e) {   
				e.printStackTrace();
			}	
			break;
		default:
			break;
		}	
		if(msg!=null){
		 //System.out.println("RuntimeTriggerReact: "+msg.toString());
		}
			return msg;			
		}
	

	public static void main(String[] args) {
	   long cookieNo=((System.currentTimeMillis()/1000)%(24*3600))*10000;
	   System.out.println(cookieNo);
	}

}
