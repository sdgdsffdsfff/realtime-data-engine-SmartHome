package state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 9, 2015 10:47:03 AM 
 * 存储各个家中传感器、家电状态
 * map< factorID, <deviceID,value>>
 */

public class HouseState2 extends HashMap<Integer, ArrayList<Integer>>  {
	int ctrolID;
	
	public HouseState2() {	}

	public int getCtrolID() {
		return ctrolID;
	}
	public void setCtrolID(int ctrolID) {
		this.ctrolID = ctrolID;
	}
	
	public void getHouseStateByCtrolID(){
		
	}

	public HouseState2(int ctrolID,
			Map<Integer, ArrayList<Integer>> factorStateList) {
		super(factorStateList);
		this.ctrolID = ctrolID;

	}	

}
