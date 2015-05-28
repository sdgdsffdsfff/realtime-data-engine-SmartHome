package trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：May 15, 2015 2:46:55 PM 
 */

public class TriggerLogic {
	//int triggerid           ;
	//List<TriggerLogicFactor>
	/** <规则ID,因素> */
	Map<Integer,List<TriggerLogicFactor>> logicMap;
	private final static String cfg_trigger_template_logic="cfg_trigger_template_logic";

	
	public void initLogicMap(MySqlClass mysql){
		this.logicMap=new HashMap<Integer, List<TriggerLogicFactor>>();
		String sql="select * from "+cfg_trigger_template_logic +";";
		String res=mysql.select(sql);
		String[] line=res.split("\n");
		for (int i = 0; i < line.length; i++) {
			String[] array=res.split(",");
			int triggerid =Integer.parseInt(array[0]);
			List<TriggerLogicFactor> factorList=null;
			if(this.logicMap.containsKey(triggerid)){
				factorList= this.logicMap.get(triggerid);
			}else{
				factorList=new ArrayList<TriggerLogicFactor>();
			}
			int seq_no    =Integer.parseInt(array[1])         ;
			int factorid1 =Integer.parseInt(array[2])            ;     
			int factorid2 =Integer.parseInt(array[3])           ; 
			String logicalrelation =array[4] ;     
			int factorid3 =Integer.parseInt(array[5])           ;
			TriggerLogicFactor fact=new TriggerLogicFactor(seq_no, factorid1, factorid2, logicalrelation, factorid3);
			factorList.add(fact);
			this.logicMap.put(triggerid, factorList);			
		}		
	}
	
	


	public static void main(String[] args) {

	}

}
