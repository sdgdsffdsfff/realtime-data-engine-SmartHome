package cooxm.state;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import cooxm.devicecontrol.device.ProfileMap;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jun 1, 2015 6:20:20 PM 
 * HashMap<factorID,level>
 */

public class LevelMap extends HashMap<Integer, Integer[]> {
	MySqlClass mysql;
	static Logger log= Logger.getLogger(LevelMap.class);
	
	LevelMap(MySqlClass mysql){
		super(getMapforDB(mysql));
		this.mysql=mysql;
		
	}
	
	static Map<Integer, Integer[]> getMapforDB(MySqlClass mysql){
		log.info("Start to initialize levelMap....");
		HashMap<Integer, Integer[]> levelMap=new HashMap<Integer, Integer[]>();
		String sql2="select  "
		+" factorid  ,"
		+"lv1,"
		+"lv2 ,"
		+"lv3 ,"
		+"lv4 ,"
		+"lv5 ,"
		+"lv6 ,"
		+"lv7 ,"
		+"lv8 ,"
		+"lv9 ,"
		+"lv10 "
		+ "  from "				
		+" dic_factor_level "
		+ ";";
		String res2=mysql.select(sql2);
		//System.out.println("get from mysql:\n"+res2);
		if(res2==null|| res2==""){
			System.err.println("ERROR:empty query by : "+sql2);
			return null;
		} 
		String[] records=res2.split("\n");
		for(String line:records){			
			Integer[] rec=new Integer[10];
			String[] index=line.split(",");
			int factorID=Integer.parseInt(index[0]);
			
			rec[0]=Integer.parseInt(index[1]);
			rec[1]=Integer.parseInt(index[2]);
			rec[2]=Integer.parseInt(index[3]);
			rec[3]=Integer.parseInt(index[4]);
			rec[4]=Integer.parseInt(index[5]);
			rec[5]=Integer.parseInt(index[6]);
			rec[6]=Integer.parseInt(index[7]);
			rec[7]=Integer.parseInt(index[8]);
			rec[8]=Integer.parseInt(index[9]);
			rec[9]=Integer.parseInt(index[10]);
			
			levelMap.put(factorID, rec);

		}
		return levelMap;		
	}
	
	public int getLevel(int factorid,double value){
		//System.out.println("factorid="+factorid+",value="+value+" -------------------------------------------");
		Integer[] levelArray=this.get(factorid);
		if( levelArray==null ||levelArray.length==0 ){
			return -1;
		}else{
			for (int i =1; i <=levelArray.length; i++) {
				int x1=levelArray[i-1];
				int x2=levelArray[i];
				if(value>=x1 && value <x2){
					return i;
				}else if(value>=levelArray[levelArray.length-1]){
					return levelArray.length;					
				}else{
					continue;
				}
			}
		}
		return -1;
	}
	
	
	
	

	public static void main(String[] args) {
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "cooxm", "cooxm");
		LevelMap lm=new LevelMap(mysql);
		int value=42000;
		int x=lm.getLevel(2501, value);
		System.out.println(value+" : "+x);

	}

}
