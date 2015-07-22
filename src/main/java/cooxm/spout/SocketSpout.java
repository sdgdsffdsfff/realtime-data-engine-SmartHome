package cooxm.spout;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.util.PraseXmlUtil;
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
	
	MongoClient mongo = null;
	DB db = null;
	DBCollection table = null;
	
	RedisThread rt;


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();

		
		Configure config=new Configure();
		this.data_server_IP=config.getValue("data_server_ip");
		this.data_server_port =Integer.parseInt(config.getValue("data_server_port"));	
		try {
			this.dataClient= new DataClient(data_server_IP,data_server_port);
			log.info("SocketSpout,connect to "+data_server_IP+":" +data_server_port+" success.");				
		} catch (IOException e) {
			log.error(e);
			try {
				this.dataClient.sock.close();
				log.error("SocketSpout,connect to "+data_server_IP+":" +data_server_port+"failed, socket will be close().");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		Thread dataThread = new Thread(this.dataClient);
		dataThread.setName("dataThread");
		dataThread.start();
		
		String mongo_server_IP=config.getValue("mongo_server_ip");
		int mongo_server_port =Integer.parseInt(config.getValue("mongo_server_port"));
		
		mongo = new MongoClient(mongo_server_IP, mongo_server_port);
		db = mongo.getDB("realTimeEngine");
		table = db.getCollection("sensorData");
		
		//rt=new RedisThread();
		//new Thread(rt).start();
	}
	

	
	@Override
	public void nextTuple() {
		Utils.sleep(10);
		String data=null;
		try {			
			if((data=DataClient.dataQueue.poll(100, TimeUnit.MILLISECONDS))!=null){	
				//System.out.println(data);
				String[] columns=data.split(",");
				String token=columns[0];				
				this.fields=xml.getColumnNames(Integer.parseInt(token));
				if (fields!=null && columns.length == fields.size()){
					toMongoDB(fields,columns);
					_collector.emit(new Values((Object[])columns ));
				}else{
					log.error("can't get Fields by key:"+columns[0]+",data:"+data);
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
		declarer.declare(new Fields("factorID","timeStamp","ctrolID","deviceID","roomType","roomID","wallID","value","rate"));
	}
	
	public void toMongoDB(List<String> fields,String[] columns){
		if(this.mongo==null ||table==null){
			return;
		}
		DBObject record = new BasicDBObject();
		for (int i = 0; i < columns.length; i++) {
			record.put(fields.get(i), columns[i]);			
		}
		table.insert(record);		
	}
	
	public static void main(String[] args) {
		
	}
	
}
