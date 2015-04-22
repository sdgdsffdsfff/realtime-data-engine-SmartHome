package state;

import java.util.HashMap;
import java.util.Map;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 9, 2015 10:47:03 AM 
 * 存储各个家中传感器、家电状态
 * map< factorID, <deviceID,value>>
 */
public class HouseState extends HashMap<Integer, HashMap<Integer, Integer>>  {
	int ctrolID;
	
	/** map< factorID, <deviceID,value>> */
	//Map<Integer, HashMap<Integer, Integer>> factorStateMap =new 	HashMap<Integer, HashMap<Integer, Integer>>();	
	

	public HouseState() {	}

	public int getCtrolID() {
		return ctrolID;
	}
	public void setCtrolID(int ctrolID) {
		this.ctrolID = ctrolID;
	}
	

	public HouseState(int ctrolID,
			Map<Integer, HashMap<Integer, Integer>> factorStateMap) {
		super(factorStateMap);
		this.ctrolID = ctrolID;
	}	
	
	

}
