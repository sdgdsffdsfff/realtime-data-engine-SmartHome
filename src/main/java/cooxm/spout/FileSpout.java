package cooxm.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import cooxm.devicecontrol.control.Configure;
import cooxm.util.PraseXmlUtil;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Feb 12, 2015 3:34:31 PM 
 */

public class FileSpout extends BaseRichSpout{
	/** **/
	private static final long serialVersionUID = 1L;
	static BufferedReader fileReader =null;
	List<String> fields=new ArrayList<String>();
	private PraseXmlUtil xml;
	private SpoutOutputCollector _collector;
	


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();
		try {
			fileReader = new BufferedReader(new FileReader(new File("data.txt")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		

	}

	@Override
	public void nextTuple() {
		Utils.sleep(5000);
		String data=null;
		try {
			if((data=fileReader.readLine())!=null){	
				System.out.println(data);
				String[] columns=data.split(",");
				int factorID=Integer.parseInt(columns[0]);
				this.fields=xml.getColumnNames(Integer.parseInt(columns[0]));
				/*String[] values=new String[columns.length];
				if(this.fields!=null){
					values[0]=columns[this.fields.indexOf("factorID")];
					values[1]=columns[this.fields.indexOf("timeStamp")];
					values[2]=columns[this.fields.indexOf("ctrolID")];
					values[3]=columns[this.fields.indexOf("deviceID")];
					values[4]=columns[this.fields.indexOf("roomType")];
					values[5]=columns[this.fields.indexOf("roomID")];
					values[6]=columns[this.fields.indexOf("wallID")];
					values[7]=columns[this.fields.indexOf("value")];					
					values[8]=columns[this.fields.indexOf("rate")];*/
				if(columns.length!=this.fields.size()){					
					System.err.println("Wrong data:"+data+",wrong number of fields.column mismatch ");
					return;
				}
				switch (Integer.parseInt(columns[0])) {   //factorID
				case 2501: //光
					//columns[7]=columns[7];
					break;
				case 2502: //PM2.5
					columns[7]=Integer.parseInt(columns[7])/100.0+"";
					break;
				case 2503: //人体探测器
					//columns[7]=columns[7];
					break;
				case 2504:  //湿度
					columns[7]=Integer.parseInt(columns[7])/100.0+"";
					break;
				case 2505:  //温度
					columns[7]=Integer.parseInt(columns[7])/100.0+"";
					break;
				case 2506:  //噪音
					columns[7]=Integer.parseInt(columns[7])/100.0+"";
					break;
				case 2507:  // 空气质量-6合1
					//columns[7]=Integer.parseInt(columns[7])/100.0+"";
					break; 
				case 201:  //烟雾探测器-一氧化碳
					//columns[7]=columns[7];
					break;
				case 211:  //漏水探测器
					//columns[7]=columns[7];
					break;			
				default:
					break;
				}
				if(factorID==541){  //空调
					columns[8]=columns[8]+","+columns[9]+","+columns[10]+","+columns[11]+","+columns[12]+","+columns[13]+","+columns[14]+","+columns[15];	
					return;
				}
				String[]  columnss={columns[0],columns[1],columns[2],columns[3],columns[4],columns[5],columns[6],columns[7],columns[8]};
				
				_collector.emit( new Values( columnss)/*,new Values(ctrolID)*/);	
				

				
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

			//declarer.declare(new Fields("factorID","timeStamp","ctrolID","deviceID","roomType","roomID","wallID","value","rate","count","onOff","mode","speed","direction","temperature","key"));  

			declarer.declare(new Fields("factorID","timeStamp","ctrolID","deviceID","roomType","roomID","wallID","value","rate"));  

		
		//declarer.declare(new Fields(this.fields));
	}

}
