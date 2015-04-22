package spout;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created��4 Jan 2015 14:46:33 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import util.PraseXmlUtil;
import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.util.MySqlClass;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class SocketSpout  extends BaseRichSpout {
	
	static Logger log =Logger.getLogger(SocketSpout.class);

	/**	 SocketSpout serialVersionUID	 */
	private static final long serialVersionUID = -4421366287283662726L;
	private SpoutOutputCollector _collector;
	private DataClient dataClient=null;
	String data_server_IP;
	int data_server_port ;
	private PraseXmlUtil xml;
	List<String> fields=new ArrayList<String>();


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();

		
		Configure config=new Configure();
		this.data_server_IP=config.getValue("data_server_ip");
		this.data_server_port =Integer.parseInt(config.getValue("data_server_port"));	
		while(true){
			try {
				this.dataClient= new DataClient(data_server_IP,data_server_port);
				log.info("SocketSpout,connect to "+data_server_IP+":" +data_server_port+"success.");				
			} catch (IOException e) {
				log.error(e);
				try {
					this.dataClient.sock.close();
					log.error("SocketSpout,connect to "+data_server_IP+":" +data_server_port+"failed, socket will be close().");
					Thread.sleep(30*1000);
					this.dataClient= new DataClient(data_server_IP,data_server_port);
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			if(this.dataClient!=null){
				this.dataClient.toQueue();	
				break;
			}
		}
	}
	

	
	@Override
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
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields("factorID","timeStamp","ctrolID","roomID","value"	));
	}
	
	public static void main(String[] args) {
		
	}
	
}
