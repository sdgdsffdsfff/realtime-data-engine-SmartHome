package cooxm.trigger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.swing.border.Border;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import clojure.lang.Compiler.NewExpr;
import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.Trigger;
import cooxm.devicecontrol.device.TriggerFactor;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Feb 5, 2015 3:55:56 PM 
 */

public class RunTimeTrigger extends Trigger{
	 public static Logger log= Logger.getLogger(RunTimeTrigger.class);
	 private static final int boundary= 3000;
	 Jedis jedis;
	
	/**所有因素都满足的触发时间，初始值：1970-01-01 00：00:00 */
	private Date triggerTime;
	/**<pre>状态：
	 * 0:没有一个因素命中；
	 * 1：所有因素没有完全命中；
	 * 2；所有因素全部命中；  */
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
	public RunTimeTrigger(Trigger trigger, Date triggerTime, int state,int timeOut) {
		super(trigger);
		SystemConfig config= SystemConfig.getConf();
		this.jedis=config.getJedis();
		this.jedis.select(9);
		this.triggerTime = triggerTime;
		this.state = state;
		this.timeOut=timeOut;
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
	public synchronized RunTimeTrigger  dataMatching(List<Object> dataLine,List<String> fields){
		//List<String> factorList=new ArrayList<String> ();
		Boolean result=null;
		int factorID=Integer.parseInt( (String) dataLine.get(0));
		int value=Integer.parseInt( (String) dataLine.get(fields.indexOf("value"))); 
		int ctrolID=Integer.parseInt( (String) dataLine.get(fields.indexOf("ctrolID"))); 
		if(!fields.contains("roomType") || !fields.contains("roomID")){
			return null;
		}
		int roomID=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomID"))); 
		int roomType=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomType"))); 
		for(TriggerFactor factor:this.getTriggerFactorList()){
            //第1个条件：因素ID相同				
			if(factor.getFactorID()!=factorID){
				continue;
			}else{
				result=true;
			}
           //第2个条件：value满足条件	
			Boolean result2=null;
			int operator=factor.getOperator();
			int min=factor.getMinValue();
			int max=factor.getMaxValue();
			switch (operator) {
			case 1:  // =
				result2=(min==value)?true:false;
				break;
			case 2: //between
				result2=(value>=min && value<=max)?true:false;
				break;
			case 3: //≠
				result2=(min==value)?false:true;
				break;
			case 4: //≥大于等于
				result2=(value>=min)?true:false;
				break;
			case 5: //>大于
				result2=(value>min)?true:false;
				break;
			case 6: //≤小于等于
				result2=(value<=min)?true:false;
				break;
			case 7:  //<小于
				result2=(value<min)?true:false;
				break;
			case 8: //not between
				result2=(value>=min && value<=max)?false:true;
				break;
			default:
				result=false;
				break;
			}
			result=result && result2;
			

			//第3个条件：roomID满足	
			if(factorID<boundary){  //数据因素匹配	
				if(factor.getRoomType()==254){  //任意房间类型
					result=result && true;				
				}else if (factor.getRoomType()==roomType && factor.getRoomID()==254) { //任意房间类型 或者ID相同
					result=result && true;	
				}else if (factor.getRoomType()==roomType && factor.getRoomID()==roomID){
					result=result && true;	
				}else{  //不满足，进入下一条
					result=false;	
					continue;
				}				
			}	
				
			if(result){
				factor.setState(true);
				factor.setCreateTime(new Date());  //因素触发时间
				this.state=1;      //已有条件满足，但是不是所有条件都满足；
				if(factorID<boundary){
					if(isDataSatisfied()){  //先判所有断数据条件是否满足要求
						systemMatch(ctrolID,roomID);
						if(isSystemSatisfied() ){
							if( this.state!=2){   //第一次触发,所有条件都满足；
								this.state=2;
								this.triggerTime=new Date();
							}
							if(this.getAccumilateTime()==0){
								return this;								
							}else if(System.currentTimeMillis()-this.triggerTime.getTime()>=this.getAccumilateTime()){ //累计时间满足
								return this;
							}							
						}
					}else{
						continue;
					}
				}				
			}else{              //如果不满足条件,则把因素 定为flase；
				factor.setState(false);				
				continue;
			}			
		}		
		return null;	 
	}	
	
	public Boolean isDataSatisfied(){
		Boolean result=true;
		Boolean orResult=null;
		for(TriggerFactor factor:this.getTriggerFactorList()){		
			if(factor.getFactorID()>=1000){  //系统因素暂时不做判断
				continue;
			}			
			String logicSign=factor.getLogicalRelation();
			if(logicSign.equalsIgnoreCase("and")){
				result=result && intToBoolean(getState());
				if(!result){
					return false;
				}
			}else if(logicSign.equalsIgnoreCase("or")){
				orResult=intToBoolean(getState()) || (Boolean)orResult ;		
			}else{
				log.error("Unknown Logical sign in Database,ctrolID:"+this.getCtrolID()
						+" triggerID:"+this.getTriggerID()
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
		for(TriggerFactor factor:this.getTriggerFactorList()){
			if(factor.getAccumilateTime()>accTime){
				accTime=factor.getAccumilateTime();
			}
		}
		return accTime;
	}
	
	/**检查是否有因素 因为超时而未触发 */
	public void checkTimeOut(){
		for(TriggerFactor factor:this.getTriggerFactorList()){
			if(this.state==1 
					&& factor.getState() 
					&& System.currentTimeMillis()-factor.getCreateTime().getTime()>=this.timeOut ){ //现在的时间 与 第一次因素触发的时间  的时间差超过 timeout秒;
				factor.setCreateTime(new Date());;
			}
		}
		
	}
	
	public void systemMatch(int ctrolID,int roomID){
		for(TriggerFactor factor:this.getTriggerFactorList()){		
			int factorID=factor.getFactorID();
			if(factorID<1000){
				continue;
			}else{
				int value=-1;
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
				case 3021:  //当前情景模式
					String key=LogicControl.currentProfile+ctrolID;
					String profileStr=this.jedis.hget(key, roomID+"");
					if(profileStr==null){
						continue ;
					}
					try {
						JSONObject json=new JSONObject(profileStr);	
						value = json.optInt("profileID");	
					} catch (JSONException e) {
						e.printStackTrace();
					}						
				default:
					break;
				}
				List<Object> dataLine=new ArrayList<Object>();
				dataLine.add(factorID);
				dataLine.add(value);
				List<String> fields=new ArrayList<String>();
				fields.add("factorID");
				fields.add("value");
				dataMatching(dataLine, fields);
			}
		}
	}
	
	public Boolean isSystemSatisfied(){
		Boolean result=true;	
		Boolean orResult=null;
		for(TriggerFactor factor:this.getTriggerFactorList()){		
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
					log.error("Unknown Logical sign in Database,ctrolID:"+this.getCtrolID()
							+" triggerID:"+this.getTriggerID()
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
	
	/**<pre> 	 * 
	 * @param dataLine 要匹配的数据
	 * @param fields 要匹配的数据的字段名
	 * @Note 规则的有效性 已经在 triggerMap加载时将无效的规则过滤掉了
	 * @return null  如果匹配失败
	 * ctrolID_triggerID_factorID 如果成功返回匹配成功的因素列表,每一个匹配到的因素id
	 * @factorID >= 1000为系统因素：
	时间：  1001
	星期几：1002
	城市(IP)：1003
   operator:
	1：= 等于
	2：between 介于
	3：≠不等于
	4：≥大于等于
	5：>大于
	6:  ≤小于等于
	7:  <小于
	8：not between不在之间*/
	public List<String> dataMatchBackup(List<Object> dataLine,List<String> fields){
		List<String> factorList=new ArrayList<String> ();
		Boolean result=null;
		int factorID=Integer.parseInt( (String) dataLine.get(0));
		int value=Integer.parseInt( (String) dataLine.get(fields.indexOf("value"))); 
		if(factorID>=1000){  //系统因素匹配
			return null;
		}else{  //数据因素匹配		
			int roomID=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomID"))); 
			int roomType=Integer.parseInt( (String) dataLine.get(fields.indexOf("roomType"))); 
			for(TriggerFactor factor:this.getTriggerFactorList()){				
                //第1个条件：因素ID相同				
				if(factor.getFactorID()!=factorID){
					continue;
				}else{
					result=true;
				}
				//第2个条件：roomID满足	
				if(factor.getRoomType()==254){  //任意房间类型
					result=result && true;				
				}else if (factor.getRoomType()==roomType && factor.getRoomID()==254) { //任意房间类型 或者ID相同
					result=result && true;	
				}else if (factor.getRoomType()==roomType && factor.getRoomID()==roomID){
					result=result && true;	
				}else{
					result=false;	
					continue;
				}
               //第3个条件：value满足条件	
				Boolean result2=null;
				int operator=factor.getOperator();
				int min=factor.getMinValue();
				int max=factor.getMaxValue();
				switch (operator) {
				case 1:  // =
					result2=(min==value)?true:false;
					break;
				case 2: //between
					result2=(value>=min && value<=max)?true:false;
					break;
				case 3: //≠
					result2=(min==value)?false:true;
					break;
				case 4: //≥大于等于
					result2=(value>=min)?true:false;
					break;
				case 5: //>大于
					result2=(value>min)?true:false;
					break;
				case 6: //≤小于等于
					result2=(value<=min)?true:false;
					break;
				case 7:  //<小于
					result2=(value<min)?true:false;
					break;
				case 8: //not between
					result2=(value>=min && value<=max)?false:true;
					break;
				default:
					result=false;
					break;
				}	
				result=result && result2;
				if(result){
					 String factorPath=this.getCtrolID()+"_"+this.getTriggerID()+"_"+factorID;
					 factorList.add(factorPath);
				}				
			}			
		}		
		return factorList;	 
	}
	
	public boolean intToBoolean(int state){
		boolean foo = false;
		if(state == 1){
		   foo = true;
		} else if(state == 0) {
		   foo = false;
		}else{
			log.error("Unknown Logical sign in Database,ctrolID:"+this.getCtrolID()
					 +" triggerID:"+this.getTriggerID() 
					 +" state:" +state);
			return foo ;
		}
		return foo;
	}
	
	public static void main(String[] args) {		

	}

}
