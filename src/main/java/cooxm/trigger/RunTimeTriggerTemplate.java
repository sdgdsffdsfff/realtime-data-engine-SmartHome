package cooxm.trigger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import javax.swing.border.Border;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import clojure.lang.Compiler.NewExpr;
import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.Device;
import cooxm.devicecontrol.device.Profile;
import cooxm.devicecontrol.device.State;
import cooxm.devicecontrol.device.Trigger;
import cooxm.devicecontrol.device.TriggerFactor;
import cooxm.devicecontrol.device.TriggerTemplate;
import cooxm.devicecontrol.device.TriggerTemplateFactor;
import cooxm.devicecontrol.device.TriggerTemplateMap;
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
			log.error(key+" not exist  in redis ,ctrolID="+ctrolID+",roomID="+roomID);
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
		//List<String> factorList=new ArrayList<String> ();
		Boolean result=null;
		
		int factorID=Integer.parseInt( (String) dataLine.get(0));
		int valueInt=Integer.parseInt( (String) dataLine.get(fields.indexOf("value"))); 
		int ctrolID=Integer.parseInt( (String) dataLine.get(fields.indexOf("ctrolID"))); 
		if(!fields.contains("roomType") || !fields.contains("roomID")){
			return -1;
		}
		int roomID=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomID"))); 
		int roomType=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomType")));
		double value=valueInt;
		switch (factorID) {
		//2015-05-04 richard 重新定义
		case 2502: //PM2.5
			//value=valueInt/100.0;
			break;
		case 2504:  //湿度
			value=valueInt/100.0;
			break;
		case 2505:  //温度
			value=valueInt/100.0;
			break;
		case 2506:  //噪音 
			value=valueInt;
			break;
		default:
			value=valueInt;
			break; 
		}
		
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){
			
			if(factorID<boundary){  //数据因素匹配	
				//第1个条件：factorID相同
				if(factor.getFactorID()==factorID ){
					result=true;
				}else{
					continue;
				}
				
				//第2个条件：设置中规则打开，且挡前运行的情景模式 和 触发规则 的模式相同；
				Profile p=getCurrentProfile(ctrolID, roomID,jedis);
				if(this.getIsAbstract()==1){               //高级设置选项
					int tSwitch= getTriggerSwitch(ctrolID, this.getTriggerTemplateID(), jedis);
					if(tSwitch==1){                     //规则打开
						result= true;
					}else{
						result=false;                     //规则关闭
						continue;
					}
				}else if(this.getIsAbstract()==0){         //隐藏在profile 设置里
					int profileTemplateID=this.getProfileTemplateID();
					
					if( profileTemplateID==254 ){  //任意情景模式都生效
						result=true;		
					}else if(p!=null && p.getProfileTemplateID()==profileTemplateID ){ //情景存在，且当前的情景模板ID和 触发规则的生效情景相同
						//result=true;	
			            //第3个条件：并且云智能打开	
						int validflag;
						if(p.getFactor(factorID)==null){
							validflag=1;
						}else{
							validflag=p.getFactor(factorID).getValidFlag();
						}
						if( validflag==1){
							result=true;
						}else{
							result=false;
							continue;
						}
					}else{   // p=null && profileTemplateID!=254
						result=false;
						continue;
					}			 
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
			}else{    //  >3000为系统因素
				switch (factorID) {				
				case 3001:  //日期因素
					DateFormat sdf=new SimpleDateFormat("yyyyMMdd");
					value=Integer.parseInt(sdf.format(new Date()));					
					break;
				case 3002:  //时间因素
					value=(int) (System.currentTimeMillis()/1000);					
					break;
				case 3003:  //星期几
					Calendar c = Calendar.getInstance();
					c.setTime(new Date(System.currentTimeMillis()));
					value = c.get(Calendar.DAY_OF_WEEK);
					break;
				default:
					break;
				}				
			}
			
           //第4个条件：value满足条件	
			Boolean result2=null;
			int operator=factor.getOperator();
			int min=factor.getMinValue();
			int max=factor.getMaxValue();
			//1：= 等于;2：≠不等于;3：between 介于[左右封闭];4：not between不在之间;5：≥大于等于;6：>大于;7:  ≤小于等于;
			//8:  <小于;9： 介于(左右都开);10: 介于[左闭右开);11: 介于(左开右闭];12: 布尔运算符'
			switch (operator) {
			case 1:  // =
				result2=(min==value)?true:false;
				break;
			case 3: //between
				result2=(value>=min && value<=max)?true:false;
				break;
			case 2: //≠
				result2=(min==value)?false:true;
				break;
			case 4: //not between
				result2=(value>=min && value<=max)?false:true;
				break;
			case 5: //≥大于等于
				result2=(value>=min)?true:false;
				break;
			case 6: //>大于
				result2=(value>min)?true:false;
				break;
			case 7: //≤小于等于
				result2=(value<=min)?true:false;
				break;
			case 8:  //<小于
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
			
			if(result){
				factor.setState(true);
				factor.setCreateTime(new Date());  //因素触发时间
				if(this.state==2 ||this.state==22){   //这一条规则之前触发过，这一次又有一条因素满足条件
					this.state=11;      //这个规则之前触发过，这一次已有条件满足，但是不是所有条件都满足；
				}else{
					this.state=1;    //第一次触发，但是不是所有条件都满足；
				}
				/*---------------------------------------- 非定时器任务 -----------------------------------*/
				if(isDataSatisfied()){  //先判所有断数据条件是否满足要求
					if( this.state==1  ){   //第一次触发,所有条件都满足；
						this.state=2;
						this.triggerTime=new Date();
						//jedis.hset(lastTriggerTime+ctrolID, this.getTriggerTemplateID()+"", this.triggerTime.getTime()+"");
					}else if( this.state==11){
						this.state=22;						
					} 
					/*long lastTriTime=Long.parseLong(jedis.hget(lastTriggerTime+ctrolID, this.getTriggerTemplateID()+""));
					long timeDiff =(new Date().getTime()-lastTriTime)/1000;
					if(timeDiff<=30*60){   //不足30分钟 则跳出；
						break;
					}else{                 //距离上次触发超过30分钟
						this.triggerTime=new Date();
						jedis.hset(lastTriggerTime+ctrolID, this.getTriggerTemplateID()+"", this.triggerTime.getTime()+"");
					}*/
					
					if(this.state==22 ){ //再次触发，判断这次触发和上次的时间差
						long timeDiff =(new Date().getTime()-this.triggerTime.getTime())/1000;
						if(timeDiff<=15*60){   //不足20分钟 则跳出；
							break;
						}else{                 //距离上次触发超过30分钟
							this.triggerTime=new Date();
						}
					}
					if(this.getAccumilateTime()==0){
						return this.getTriggerTemplateID();								
					}else if(System.currentTimeMillis()-this.triggerTime.getTime()>=this.getAccumilateTime()){ //累计时间满足
						return this.getTriggerTemplateID();
					}
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
				continue;
			}			
		}		
		return -1;	 
	}	
	
/*	public Boolean isDataSatisfied(){
		Boolean result=true;
		Boolean orResult=null;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){		
			String logicSign=factor.getLogicalRelation();
			if(logicSign.equalsIgnoreCase("and")){
				result=result && intToBoolean(getState());
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
		if(orResult==null){  //没有or的因素
			return result;
		}else{
			return result && orResult;
		}	
	}*/
	
	public Boolean isDataSatisfied(){
		Boolean result=true;
		Boolean orResult=null;
		for(TriggerTemplateFactor factor:this.getTriggerTemplateFactorList()){		
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
					result= intToBoolean(getState()) && result;
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
	
		/*TriggerTemplateMap triggerMap = new TriggerTemplateMap(config.getMysql());
		System.out.println(triggerMap.size());
		
		
		RunTimeTriggerTemplate r=new RunTimeTriggerTemplate(triggerMap.get(1), new Date(), 0, 0);
		Profile a = r.getCurrentProfile(1256783, 101);
		System.out.println(a.toJsonObj().toString());*/
		
		Jedis jedis=config.getJedis();
		jedis.select(9);
		System.out.println(new Date());
		//for (int i = 0; i < 10000; i++) {
			String s=jedis.hget("roomBind:40006", "1011");
		//}
		System.out.println(s);
		

		
	}

}
