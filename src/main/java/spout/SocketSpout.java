package spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import util.PraseXmlUtil;
import util.SystemConfig;
import cooxm.devicecontrol.control.Config;
import cooxm.devicecontrol.util.MySqlClass;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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
	List<String> fields;
	String file="data.txt";
	//private Config config;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();
//		SystemConfig config= SystemConfig.getConf();
//		this.sock=config.getDataClient();
//		this.sock.toQueue();	
		

	}
	
	@Override
	public void nextTuple() {
		String data=null;
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader(new File(file)));
			//if((data=DataClient.dataQueue.poll(1000, TimeUnit.MILLISECONDS))!=null){
			if((data=fileReader.readLine())!=null){
				String[] columns=data.split(",");
				String token=columns[0];
				this.fields=xml.getColumnNames(Integer.parseInt(token));
				if (columns.length == fields.size()){
					_collector.emit(new Values((Object[])columns));
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
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields(this.fields));
	}
	
	public static void main(String[] args) {
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");
	}
	
}
