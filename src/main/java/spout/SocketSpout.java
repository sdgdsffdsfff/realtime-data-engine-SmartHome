package spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import util.PraseXmlUtil;
import cooxm.devicecontrol.util.MySqlClass;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created��4 Jan 2015 14:46:33 
 */

public class SocketSpout  extends BaseRichSpout {

	/**	 SocketSpout serialVersionUID	 */
	private static final long serialVersionUID = -4421366287283662726L;
	private SpoutOutputCollector _collector;
	private DataClient sock=null;
	private PraseXmlUtil xml;
	List<String> fields=new ArrayList<String>();
	BufferedReader fileReader =null;
	//private Config config;
	//String file="data.txt";

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();
		try {
			this.fileReader = new BufferedReader(new FileReader(new File("data.txt")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
//		SystemConfig config= SystemConfig.getConf();
//		this.sock=config.getDataClient();
//		this.sock.toQueue();	
	}
	
	@Override
	public void nextTuple() {
		Utils.sleep(500);
		String[] values=new String[5];
		String data=null;
		try {
			if((data=fileReader.readLine())!=null){				
				String[] columns=data.split(",");				
				this.fields=xml.getColumnNames(Integer.parseInt(columns[0]));
				values[0]=columns[this.fields.indexOf("factorID")];
				values[1]=columns[this.fields.indexOf("timeStamp")];
				values[2]=columns[this.fields.indexOf("ctrolID")];
				values[3]=columns[this.fields.indexOf("roomID")];
				values[4]=columns[this.fields.indexOf("value")];
				
				_collector.emit( new Values( values));	
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
/*	@Override
	public void nextTuple() {
		Utils.sleep(100);
		String data=null;
		try {			
			if((data=DataClient.dataQueue.poll(1000, TimeUnit.MILLISECONDS))!=null){		
				String[] columns=data.split(",");
				String token=columns[0];				
				this.fields=xml.getColumnNames(Integer.parseInt(token));
				if (columns.length == fields.size()){
					_collector.emit(new Values((Object[])columns ));	 				
				}else{
					return;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}*/

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields("factorID","timeStamp","ctrolID","roomID","value"	));
	}
	
	public static void main(String[] args) {
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");
	}
	
}
