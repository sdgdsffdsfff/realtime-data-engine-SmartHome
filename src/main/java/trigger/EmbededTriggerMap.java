package trigger;

import java.util.Date;
import java.util.HashMap;
import java.util.function.Predicate;
import util.GeneralMethod;
import util.SystemConfig;
import cooxm.devicecontrol.device.Trigger;
import cooxm.devicecontrol.device.TriggerFactor;
import cooxm.devicecontrol.device.TriggerMap;
import cooxm.devicecontrol.util.BytesUtil;
import cooxm.devicecontrol.util.MySqlClass;

/*
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：4 Feb 2015 17:36:17 
 */


/**
将Map<ctrolID_triggerID,trigger>改造为Map<ctrolID,Map<triggerID,trigger>嵌套结构
 */
public class EmbededTriggerMap extends HashMap<Integer, HashMap<Integer,RunTimeTrigger>>{
	private static final long serialVersionUID = 1L;
	
	/**
	将Map<ctrolID_triggerID,trigger>改造为Map<ctrolID,Map<triggerID,trigger>嵌套结构
	 */
	public EmbededTriggerMap(TriggerMap triggerMap){
		HashMap<Integer, RunTimeTrigger> triggerList=null;
		for (Entry<String,Trigger>entry: triggerMap.entrySet()){
			//Trigger trigger=(Trigger) GeneralMethod.depthClone(entry.getValue());
			Trigger trigger=entry.getValue();
			RunTimeTrigger runTrigger=new RunTimeTrigger(trigger,new Date(0),0, SystemConfig.getConf().getTriggerTimeOut());
			final Predicate< TriggerFactor> filter=new Predicate<TriggerFactor>() {
				@Override
				public boolean test(TriggerFactor t) {
					if(t.getValidFlag()==0){
					    return true;
					}else{
						return false;
					}
				}				
			};
			runTrigger.getTriggerFactorList().removeIf(filter);  //移除所有无效的factor
			if(0==trigger.getTriggerFactorList().size()){  //如果全部无效，则该trigger无效，跳出，进入下一个trigger
				continue;
			}

			
			String[] key=entry.getKey().split("_");
			int ctrolID=Integer.parseInt(key[0]);
			int triggerID=Integer.parseInt(key[1]);
			if(this.containsKey(ctrolID)){
				triggerList=this.get(ctrolID);				
			}
			else{
				triggerList=new HashMap<Integer,RunTimeTrigger>();					
			}
			triggerList.put(triggerID, runTrigger);
			this.put(ctrolID, triggerList);			
		}		
	}




	public static void main(String[] args) {
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");
		TriggerMap rm=new TriggerMap(mysql);
		EmbededTriggerMap dm=new EmbededTriggerMap(rm);
		System.out.println(dm.size());

	}

}
