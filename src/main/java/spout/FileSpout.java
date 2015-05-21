package spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import util.PraseXmlUtil;
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
			fileReader = new BufferedReader(new FileReader(new File("data1.txt")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		String data=null;
		try {
			if((data=fileReader.readLine())!=null){	
				String[] columns=data.split(",");	
				/*String[] values=new String[columns.length];
				this.fields=xml.getColumnNames(Integer.parseInt(columns[0]));
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
				if(columns.length!=9){
					System.err.println("Wrong data:"+data);
				}
					
					_collector.emit( new Values( columns)/*,new Values(ctrolID)*/);	
					//System.out.println(data);
				//}
				
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
		declarer.declare(new Fields("factorID","timeStamp","ctrolID","deviceID","roomType","roomID","wallID","value","rate"));  //"factorID","timeStamp","ctrolID","roomID","value"
	}

}
