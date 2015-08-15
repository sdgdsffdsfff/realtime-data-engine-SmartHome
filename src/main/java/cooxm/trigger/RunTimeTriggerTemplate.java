package cooxm.trigger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import javax.swing.border.Border;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.org.apache.bcel.internal.generic.NEW;

import clojure.lang.Compiler.NewExpr;
import redis.clients.jedis.Jedis;
import cooxm.bolt.MatchingBolt2;
import cooxm.bolt.ReactBolt;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.Device;
import cooxm.devicecontrol.device.Factor;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.State;
import cooxm.devicecontrol.device.Trigger;
import cooxm.devicecontrol.device.TriggerFactor;
import cooxm.devicecontrol.device.TriggerTemplate;
import cooxm.devicecontrol.device.TriggerTemplateFactor;
import cooxm.devicecontrol.device.TriggerTemplateMap;
import cooxm.devicecontrol.socket.Message;
import cooxm.spout.DataClient;
import cooxm.spout.SocketSpout;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Feb 5, 2015 3:55:56 PM 
 */

public class RunTimeTriggerTemplate  extends TriggerTemplate{
	 public static Logger log= Logger.getLogger(RunTimeTriggerTemplate.class);
	 private static final int boundary= 3000;
	 Map<String,Thread> taskMap;
	 private static final String lastTriggerTime="lastTriggerTime:"; //在redis 记录这条规则最后一次触发的时间
	 
	 static long cookieNo=((System.currentTimeMillis()/1000)%(24*3600))*10000;
	 
	 //Jedis jedis;
	/**所有因素都满足的触发时间，初始值：1970-01-01 00：00:00 */
	private Date triggerTime;
	/**<pre>状态：
	 * 0:没有一个因素命中；
	 * 1：所有因素没有完全命中；
	 * 2；所有因素全部命中； 
	 * 11：这个规则之前触发过，这次所有因素部分命中
	 * 22： 这个规则之前触发过，这次所有因素全部命中*/
	private int  state;	
	/** 超时未触发，则已经满足条件的因素失效 */
	private int timeOut;
	
	public Date getTriggerTime() {
		return triggerTime;
	}
	public void setTriggerTime(Date triggerTime) {
		this.triggerTime = triggerTime;
	}
	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}
	public int getTimeOut() {
		return timeOut;
	}
	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}
	public RunTimeTriggerTemplate(TriggerTemplate trigger, Date triggerTime, int state,int timeOut) {
		super(trigger);
		//SystemConfig config= SystemConfig.getConf();
		//this.jedis=config.getJedis();this.jedis.select(9);
		
		this.taskMap=new HashMap<String, Thread>();

		this.triggerTime = triggerTime;
		this.state = state;
		this.timeOut=timeOut;
	}
	
	
	public RunTimeTriggerTemplate() {
	}
	
	public  Profile getCurrentProfile(int ctrolID,int roomID,Jedis jedis){
		String key=LogicControl.currentProfile+ctrolID;
		jedis.select(9);
		String p=jedis.hget(key, roomID+"");
		if(p==null || p==""){
			log.error(key+" not exist  in redis,ctrolID="+ctrolID+",roomID="+roomID);
			return null;
		}
		JSONObject json;
		try {
			json = new JSONObject(p);
			Profile profile=new Profile(json);
			return profile;
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public  Integer[] getStableTemparatureFromRedis(int ctrolID,int roomID,Jedis jedis){
		String key=LogicControl.currentDeviceState+ctrolID;
		jedis.select(9);
		Map<String, String> sMap=jedis.hgetAll(key);
		if(sMap==null ){
			log.error(key+" not exist  in redis ,ctrolID="+ctrolID+",roomID="+roomID);
			return new Integer[]{-1,-1,-1};
		}
		JSONObject json;
		for (Map.Entry<String, String> entry:sMap.entrySet()) {
			try {
				json = new JSONObject(entry.getValue());
				if(json.has("state")){  //空调
					String devStr=jedis.hget(LogicControl.roomBind+ctrolID, entry.getKey());
					if(devStr==null){
						//return new Integer[]{-1,-1,-1};
						continue;
					}else{
						int sender=json.getInt("sender");
						Device d=new Device(new JSONObject(devStr));
						if(d.getRoomID()==roomID){   //找到了房间的空调
							int stable=json.getJSONObject("state").getInt("stable" );
							int onOff=json.getJSONObject("state").getInt("onOff");

							onOff=(onOff==0?501:502);
							return new Integer[]{stable,onOff,sender};
						}
					}
				}
				
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return new Integer[]{-1,-1,-1};
	}
	
	public  Date getCurrentProfileSwitchTime(int ctrolID,int roomID,Jedis jedis){
		DateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String key=LogicControl.currentProfile+ctrolID;
		jedis.select(9);
		String p=jedis.hget(key, roomID+"");
		JSONObject json;
		try {
			json = new JSONObject(p);
			String timeStr=json.optString("time");
			if(timeStr!=null){
				Date time =sdf.parse(timeStr);
				return time;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/** 从redis获取trigger 的开关，如果获取不到 则默认开关打开
     */
	public  int getTriggerSwitch(int ctrolID,int triggerID,Jedis jedis){
		String key=LogicControl.currentProfile+ctrolID;
		jedis.select(9);
		String p=jedis.hget(key, triggerID+"");
		if(p==null || p==""){   //默认都是打开的
			return 1;
		}else{
			JSONObject json;
			try {
				json = new JSONObject(p);
				int validFlag=json.getInt("validFlag");
				return validFlag;
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return 1;		
	}

	
	/**<pre> 
	 * 不改变 trigger本身的state情况下去匹配
	 * @param dataLine 要匹配的数据
	 * @param fields 要匹配的数据的字段名
	 * @return 
	 * @Note 规则的有效性 已经在 triggerMap加载时将无效的规则过滤掉了
	 * @return null  如果匹配失败
	 * ctrolID_triggerID_factorID 如果成功返回匹配成功的因素列表,每一个匹配到的因素id
	 * @factorID >= boundary为系统因素,没有数据推送，需要主动查询
	3001	日期
	3002	时间
	3003	星期几
	3011	省份
	3012	城市
	3021	当前情景模式类型
   operator:
	1：= 等于
	2：between 介于
	3：≠不等于
	4：≥大于等于
	5：>大于
	6:  ≤小于等于
	7:  <小于
	8：not between不在之间*/
	public synchronized int  dataMatching(List<Object> dataLine,List<String> fields,Jedis jedis){
		Boolean result=null;
		
		int factorID=Integer.parseInt(  dataLine.get(0)+"");
		Double valueInt=Double.parseDouble(  dataLine.get(fields.indexOf("value"))+""); 
		int ctrolID=Integer.parseInt( (String) dataLine.get(fields.indexOf("ctrolID"))); 
		if(!fields.contains("roomType") || !fields.contains("roomID")){
			return -1;
		}
		int roomID=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomID"))); 
		int roomType=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomType")));
		double value=valueInt;
		
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){
			
			if(factor.getFactorID()<boundary){  //数据因素匹配	
				//第1个条件：factorID相同
				if(factor.getFactorID()==factorID ){
					result=true;
				}else{
					continue ;
				}
				
				//第2个条件：设置中规则打开，且挡前运行的情景模式 和 触发规则 的模式相同；
				if(this.getIsAbstract()==1){               //高级设置选项
					int tSwitch= getTriggerSwitch(ctrolID, this.getTriggerTemplateID(), jedis);
					if(tSwitch==1){                     //规则打开
						result= true;
					}else{
						result=false;                     //规则关闭
						continue;
					}
				}
				
				Profile currProfile=null;
				int profileTemplateID=this.getProfileTemplateID();				
				if( profileTemplateID==254 ){  //任意情景模式都生效
					result=true;		
				}else {
					currProfile=getCurrentProfile(ctrolID, roomID,jedis);
					if(currProfile!=null && currProfile.getProfileTemplateID()==profileTemplateID ){ //情景存在，且当前的情景模板ID和 触发规则的生效情景相同
						//result=true;	
			            //第3个条件：并且云智能打开	
						int validflag;
						if(currProfile.getFactor(factorID)==null){
							validflag=1;
						}else{
							validflag=currProfile.getFactor(factorID).getValidFlag();
						}
						if( validflag==1){
							result=true;
						}else{
							result=false;
							continue;
						}
					}else{   // p=null && profileTemplateID!=254
						result=false ;
						continue;
					}					
				}
				
				switch (factorID) {
				case 2505:  //温度
					if(this.getTriggerTemplateID()==101 ||this.getTriggerTemplateID()==102){
						Integer[] stable_OnOFF_Sender =getStableTemparatureFromRedis(ctrolID, roomID,jedis);
						int stable=stable_OnOFF_Sender[0];
						int onOff=stable_OnOFF_Sender[1];
						int sender=stable_OnOFF_Sender[2];
						if(stable==-1 && sender==-1){
							
						}
						if((sender==0 || sender==1 )&& onOff==502){  //用户关闭并且
							return -1 ;
						}
						if(currProfile==null){
							currProfile=getCurrentProfile(ctrolID, roomID,jedis);
						}
						if(currProfile!=null){
							int air_flag=0;
							for (Factor fa : currProfile.getFactorList()) {
								if(fa.getFactorID()==541){									
									if(stable>0){               //以遥控面板为准
										factor.setMaxValue(stable+3);
										factor.setMinValue(stable-3); 
									}else{                       //以情景设置为准
										factor.setMaxValue(fa.getMaxValue()+3);
										factor.setMinValue(fa.getMinValue()-3);
									}
									air_flag=1;
								}else{     //情景中没有空调的设置项
									continue;
								}
							}
							if (air_flag==0) {  //没有空调设置
								if(stable>0){               //遥控面板也没有设置
									factor.setMaxValue(stable+3);
									factor.setMinValue(stable-3); 
								}else{
									continue;
								}
							}
						}else{  //当前情景模式不存在,匹配失败
							if(stable>0){               //遥控面板也有设置
								factor.setMaxValue(stable+3);
								factor.setMinValue(stable-3); 
							}else{
								return -1;
							}
						}
					}
					break;
				default:
					break; 
				}			
				
				//第3个条件：roomType满足	
				if(factor.getRoomType()==254){  //任意房间类型
					result=result && true;				
				}else if (factor.getRoomType()==roomType ) { //任意房间类型 或者ID相同
					result=result && true;	
				}else{  //不满足，匹配下一个条件
					result=false;	
					continue;
				}	
				
		           //第4个条件：value满足条件	
				Boolean result2=null;
				int operator=factor.getOperator();
				int min=factor.getMinValue();
				int max=factor.getMaxValue();
				//1：= 等于;2：≠不等于;3：between 介于[左右封闭];4：not between不在之间;5：≥大于等于;6：>大于;7:  ≤小于等于;
				//8:  <小于;9： 介于(左右都开);10: 介于[左闭右开);11: 介于(左开右闭];12: 布尔运算符'
				switch (operator) {
				case 1: // =
					result2=(min==value)?true:false;
					break;
				case 3: // between
					if(factor.getFactorID()==3002 && factor.getMaxValue()<factor.getMinValue()){
						result2=(value>=min || value<=max)?true:false;
					}else{
						result2=(value>=min && value<=max)?true:false;
					}
					
					break;
				case 2: // ≠
					result2=(min==value)?false:true;
					break;
				case 4: // not between
					result2=(value>=min && value<=max)?false:true;
					break;
				case 5: // ≥大于等于
					result2=(value>=min)?true:false;
					break;
				case 6: // >大于
					result2=(value>min)?true:false;
					break;
				case 7: // ≤小于等于
					result2=(value<=min)?true:false;
					break;
				case 8:  // <小于
					result2=(value<min)?true:false;
					break;
				case 9:  // 介于开区间
					result2=(value>min && value<max)?true:false;
					break;
				case 10:  //介于左闭又开
					result2=(value>=min && value<max)?true:false;
					break;
				case 11:  //介于左开右闭
					result2=(value>min && value<=max)?true:false;
					break;
				case 12:  // 逻辑运算
					result2=(min==value)?true:false;
					break;
				default:
					result=false;
					break;
				}				
				result=result && result2;				
			} /*else if(factor.getFactorID()>boundary && isDataSatisfied()){    //  >3000为系统因素
				result=SystemMatch(factor,ctrolID, roomID, jedis);
			}*/else{
				result=false;
			}

			if(result ){
				if(factor.getState()==null || factor.getState()==false){     //只记录因素第一次触发时间
					factor.setState(true);
					factor.setCreateTime(new Date());  //因素触发时间
				}
				if(this.state==2 ||this.state==22 ||this.state==11){   //这一条规则之前触发过，这一次又有一条因素满足条件
					this.state=11;          //这个规则之前触发过，这一次已有条件满足，但是不是所有条件都满足；
				}else{
					this.state=1;           //第一次触发，但是不是所有条件都满足；
				}
				/*---------------------------------------- 非定时器任务 -----------------------------------*/
				if(isDataSatisfied()  ){   //先判所有断数据条件是否满足要求
					if (!isSystemSatisfied()) { //没有系统匹配
						for(TriggerTemplateFactor fr:this.getTriggerTemplateFactorList()){
							if(fr.getFactorID()>boundary){
								SystemMatch(fr, ctrolID, roomID, jedis);
							}
						}
					}					
					Boolean systemFlag=isSystemSatisfied();
					if (systemFlag==false ||systemFlag==null ) {
						//System.out.println("failed:system not match ------------------");
						continue ;
					}
					//System.out.println("triggered:"+this.getTriggerTemplateID()+" "+this.getTriggerName()+",this.state=      "+this.state);
					if( this.state==1  ){   //第一次触发,所有条件都满足；
						this.state=2;
						this.triggerTime=new Date();
					}else if( this.state==11){
						this.state=22;	
					}
					
					long timeDiff=(System.currentTimeMillis()-this.triggerTime.getTime())/1000;
					//System.out.println("timeDiff="+timeDiff);
					if(this.getAccumilateTime()==0 || timeDiff >=this.getAccumilateTime()){ //累计时间满足,触发成功
						
					}else{
						break;
					}
					
					if(this.state==22 ){ //再次触发，判断这次触发和上次的时间差
						timeDiff =(new Date().getTime()-this.triggerTime.getTime())/1000;
						if(factor.getInterval()!=0 && timeDiff<=factor.getInterval()){   //不足20分钟 则跳出；
							break;
						}else {                 //距离上次触发超过30分钟 则触发
							this.triggerTime=new Date();
						}
					}

					for (TriggerTemplateFactor factor2:this.getTriggerTemplateFactorList()) {
						factor2.setState(false);          //将因素触发状态 置0;
					}
					if(this.getTriggerTemplateID()==120  && this.state==2){  //居家模式切换到睡眠模式预启动
						switchDevice( jedis, ctrolID, roomID, 411);  //遥控窗
						switchDevice( jedis, ctrolID, roomID, 421);  //遥控窗帘
						this.triggerTime=new Date();
						this.setCreateTime(new Date());
						return -1;                                   //返回不再进行到ReactBolt
					}
					
					if(this.getTriggerTemplateID()==120  && this.state==22){  //向spout的队列添加一条消息
						DateFormat sdf3=new SimpleDateFormat("yyyyMMddHHmmssSSS");
						String s=120+","+sdf3.format(new Date())+","+ctrolID+","+0+","+roomType+","+roomID+","+0+","+501+","+0;
						DataClient.dataQueue.add(s);  
						
						System.out.println("sending :"+s);
					}
					
					return this.getTriggerTemplateID();								

				}else{
					continue;
				}
				/*------------------------------------------ 非定时器任务结束 ------------------------------------*/
				
				
				/*---------------------------------------- 定时器任务 -----------------------------------
				if(isDataSatisfied()){  //先判所有断数据条件是否满足要求
					if( this.state!=2){   //第一次触发,所有条件都满足；
						this.state=2;
						this.triggerTime=new Date();
					}
					if(this.getAccumilateTime()==0){
						return this.getProfileTemplateID();								
					}else {
						TriggerTimeOutTask task = new TriggerTimeOutTask(ctrolID,roomID,this.getTriggerTemplateID(),this.getAccumilateTime());
						Thread th=new Thread(task);
						th.start();
						this.taskMap.put(ctrolID+"_"+roomID+"_"+this.getTriggerTemplateID(), th);
					}
				}else{  //
					String key=ctrolID+"_"+roomID+"_"+this.getTriggerTemplateID();
					if(this.taskMap.containsKey(key)){
						this.taskMap.get(key).interrupt();
						this.taskMap.remove(key);
					}
				}
				------------------------------------------ 定时器任务结束 ------------------------------------*/
			
			}else{              //如果不满足条件,则把因素 定为flase；
				factor.setState(false);	
				if(this.getTriggerTemplateID()==120 && factor.getFactorID()<boundary){      // 如果非系统因素
					this.state=0;
				}else if(this.getTriggerTemplateID()==121){ //状态从置为2， 24小时内永远不再执行；
					this.state=2;
				}
				continue;
			}			
		}		
		return -1;	 
	}
	
	public  Boolean SystemMatch(TriggerTemplateFactor factor,int ctrolID,int roomID,Jedis jedis){
		Boolean result=null;
		double value;	
		int factorID=factor.getFactorID();
		if(factorID<boundary){
			return false;
		}
		Profile p=getCurrentProfile(ctrolID, roomID,jedis);
		DateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		DateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar calender=Calendar.getInstance();  	
		switch (factorID) {				
		case 3001:  //日期因素					
			value=Integer.parseInt(sdf.format(new Date()));					
			break;
		case 3002:  //时间因素
			
			value= calender.get(Calendar.HOUR_OF_DAY)*100+calender.get(Calendar.MINUTE);//
			int targetValue=factor.getMaxValue();
			
			if(targetValue<2459){   //系统时间
				if (factor.getMinValue()<=factor.getMaxValue()) {
					result= (value>=factor.getMinValue() && value<=factor.getMinValue())?true:false;
				}else{
					result= (value>=factor.getMinValue() || value<=factor.getMinValue())?true:false;
				}
				factor.setState(result);
				return result;
			}
			switch (targetValue) {
			case 3031:  //闹钟时间
				if(p.getProfileTemplateID()==1){  //睡眠模式
					for (Factor ft:p.getFactorList()) {
						if(ft.getFactorID()==3031){ //闹钟时间
							if(value>=ft.getMinValue()){
								result=true;
							}else{
								result=false;
							}
						}else{
							result=false;
						}
					}	
				}else{
					result=false;
				}
				factor.setState(result);
				break;
			case 4001:  //睡眠模式启动时间
				if(p.getProfileTemplateID()==1){  //睡眠模式
					String pStr=jedis.hget(LogicControl.currentProfile, roomID+"");						
					try {
						if(pStr!=null){
							String times=new JSONObject(pStr).getString("switchTime");
							Date switchTime=sdf2.parse(times);
							long timeDiff=(calender.getTime().getTime()-switchTime.getTime())/1000;
							if(timeDiff>=300){  //睡眠模式后5分钟
								result=true;
							}else{
								result=false;
							}
							factor.setState(result);
						}
					} catch (JSONException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
				break;
			case 4002:  //观影模式启动时间
				if(p.getProfileTemplateID()==2){  //睡眠模式
					String pStr=jedis.hget(LogicControl.currentProfile, roomID+"");						
					try {
						if(pStr!=null){
							String times=new JSONObject(pStr).getString("switchTime");
							Date switchTime=sdf2.parse(times);
							long timeDiff=(calender.getTime().getTime()-switchTime.getTime())/1000;
							if(timeDiff>=300){  //观影模式后5分钟
								result=true;
							}else{
								result=false;
							}
							factor.setState(result);
						}
					} catch (JSONException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
				break;
			case 4003:  //离家模式启动时间
				if(p.getProfileTemplateID()==3){  //睡眠模式
					String pStr=jedis.hget(LogicControl.currentProfile, roomID+"");						
					try {
						if(pStr!=null){
							String times=new JSONObject(pStr).getString("switchTime");
							Date switchTime=sdf2.parse(times);
							long timeDiff=(calender.getTime().getTime()-switchTime.getTime())/1000;
							if(timeDiff>=300){  //观影模式后5分钟
								result=true;
							}else{
								result=false;
							}
							factor.setState(result);
						}
					} catch (JSONException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}						
				break;
			case 4004:  //居家模式启动时间
				if(p.getProfileTemplateID()==2){  //睡眠模式
					String pStr=jedis.hget(LogicControl.currentProfile, roomID+"");						
					try {
						if(pStr!=null){
							String times=new JSONObject(pStr).getString("switchTime");
							Date switchTime=sdf2.parse(times);
							long timeDiff=(calender.getTime().getTime()-switchTime.getTime())/1000;
							if(timeDiff>=300){  //观影模式后5分钟
								result=true ;
							}else{
								result=false;
							}
							factor.setState(result);
						}
					} catch (JSONException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}						
				break;

			default:
				break;
			}
			break;
		case 3003:  //星期几
			Calendar c = Calendar.getInstance();
			value = c.get(Calendar.DAY_OF_WEEK);
			break;
		case 4005:  //某一规则的触发时间
			value= calender.getTime().getTime()/1000;
			TriggerTemplateMap triggerMap=MatchingBolt2.runTriggerMap.get(ctrolID+"_"+roomID);
			if(triggerMap!=null){
				int triggerID=factor.getMinValue();
				TriggerTemplate tr = triggerMap.get(triggerID);
				RunTimeTriggerTemplate trigger=null;
				if (tr instanceof RunTimeTriggerTemplate) {
					 trigger=(RunTimeTriggerTemplate)triggerMap.get(triggerID);
				}else{
					return false;
				}
				long timeDiff=(long) (value-trigger.triggerTime.getTime()/1000);
				if((trigger.getState()==2 ||trigger.getState()==22) && timeDiff>=0){
					result=true;
				}else{
					result=false;
				}
				factor.setState(result);
			}						
			break;
		default:
			break;
		}	
		return result ;

	}
	
	public static void switchDevice(Jedis jedis,int ctrolID,int roomID,int deviceType){
		List<Device> deviceList=Device.getDeviceFromRedisByType(jedis, ctrolID, roomID, deviceType);
		for (Device device : deviceList) {
			String cookie=cookieNo++ +"_5";
			JSONObject json=new JSONObject();
			try {
				json.put("ctrolID", ctrolID);
				json.put("roomID", roomID);
				json.put("deviceID", device.getDeviceID());
				json.put("deviceType", device.getDeviceType());
				json.put("sender",5);
				json.put("receiver",0); 
				json.put("keyType", 502);
				Message msg=new Message((short) (LogicControl.SWITCH_DEVICE_STATE), cookie,json );
				msg.writeBytesToSock2(ReactBolt.deviceControlServer.sock);
				log.info("send pre-trigger command,ctrolID="+ctrolID+"deviceType="+deviceType+",commandID="+Integer.toHexString(msg.getCommandID())+",msg:"+msg.toString());
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}
	
	public Boolean isAllSatisfied(){
		Boolean result=true;
		Boolean orResult=null;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){	
			int factorID=factor.getFactorID();
			if(factorID>boundary){
				continue;
			}else{
				String logicSign=factor.getLogicalRelation();
				if(logicSign.equalsIgnoreCase("and")){
					if(factor.getState()!=null){
						result=result && factor.getState();
					}else{
						result=result && false;
					}
	
					if(!result){
						return false;
					}
				}else if(logicSign.equalsIgnoreCase("or")){
					orResult=intToBoolean(getState()) || (Boolean)orResult ;		
				}else{
					log.error("Unknown Logical sign in Database"
							+" triggerID:"+this.getTriggerTemplateID()
							+" factorID:"+factor.getFactorID());
					return false;
				}			
			}	
	
		}
		if(orResult==null){  //没有or的因素
			return result;
		}else{
			return result && orResult;
		}
	}
	
	public Boolean isDataSatisfied(){
		Boolean result=true;
		Boolean orResult=null;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){	
			int factorID=factor.getFactorID();
			if(factorID>boundary){
				continue;
			}else{
				String logicSign=factor.getLogicalRelation();
				if(logicSign.equalsIgnoreCase("and")){
					if(factor.getState()!=null){
						result=result && factor.getState();
					}else{
						result=result && false;
					}
	
					if(!result){
						return false;
					}
				}else if(logicSign.equalsIgnoreCase("or")){
					orResult=intToBoolean(getState()) || (Boolean)orResult ;		
				}else{
					log.error("Unknown Logical sign in Database"
							+" triggerID:"+this.getTriggerTemplateID()
							+" factorID:"+factor.getFactorID());
					return false;
				}			
			}	
	
		}
		if(orResult==null){  //没有or的因素
			return result;
		}else{
			return result && orResult;
		}
	}
	

	
	public Boolean isSystemSatisfied(){
		Boolean result=true;	
		Boolean orResult=null;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){		
			int factorID=factor.getFactorID();
			if(factorID<boundary){
				continue;
			}else{			
				String logicSign=factor.getLogicalRelation();
				if(logicSign.equalsIgnoreCase("and")){
					if(factor.getState()!=null){
						result=result && factor.getState();
					}else{
						result=result && false ;
					}
					if(!result){
						return false;
					}
				}else if(logicSign.equalsIgnoreCase("or")){
					orResult=intToBoolean(getState()) || (Boolean)orResult ;		
				}else{
					log.error("Unknown Logical sign in Database"
							+" triggerID:"+this.getTriggerTemplateID()
							+" factorID:"+factor.getFactorID());
					return false;
				}			
			}	
		}
		if(orResult==null){  //没有or的因素
			return result;
		}else{
			return result && orResult;
		}		
	}
	
	public int getAccumilateTime(){
		int accTime=0;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){
			if(factor.getAccumilateTime()>accTime){
				accTime=factor.getAccumilateTime();
			}
		}
		return accTime;
	}
	
	/**检查是否有因素 因为超时而未触发 */
	public void checkTimeOut(){
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){
			if(this.state==1 
					&& factor.getState() 
					&& System.currentTimeMillis()-factor.getCreateTime().getTime()>=this.timeOut ){ //现在的时间 与 第一次因素触发的时间  的时间差超过 timeout秒;
				factor.setCreateTime(new Date());;
			}
		}
		
	}

	
	public boolean intToBoolean(int state){
		boolean foo = false;
		if(state == 1){
		   foo = true;
		} else if(state == 0) {
		   foo = false;
		}else{
			log.error("Unknown Logical sign in Database"
					 +" triggerID:"+this.getTriggerTemplateID()
					 +" state:" +state);
			return foo ;
		}
		return foo;
	}
	
	public static void main(String[] args)  {
		SystemConfig config= SystemConfig.getConf();
		Jedis jedis=config.getJedis();
		jedis.select(9);
		TriggerTemplateMap triggerMap = new TriggerTemplateMap(config.getMysql());
		System.out.println(triggerMap.size());
		
		
		RunTimeTriggerTemplate r=new RunTimeTriggerTemplate(triggerMap.get(101), new Date(), 0, 0);
		Profile a = r.getCurrentProfile(1256783, 101,jedis);
		System.out.println(a.toJsonObj().toString());
		

//		jedis.select(9);
//		System.out.println(new Date());
//		//for (int i = 0; i < 10000; i++) {
//			String s=jedis.hget("roomBind:40006", "1011");
//		//}
//		System.out.println(s);
		

		
	}

}
