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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.sf.json.util.NewBeanInstanceStrategy;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.DeviceState;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.util.PraseXmlUtil;
import cooxm.util.RedisUtil;
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
	
	//RedisThread rt;
	Jedis jedis;


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
		
		String redis_ip         =config.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(config.getValue("redis_port"));
		//this.jedis=new Jedis(redis_ip, redis_port,10000);
		this.jedis=RedisUtil.getJedis();
		jedis.select(9);
		
		//rt=new RedisThread();
		//new Thread(rt).start();
	}
	

	
	@Override
	public void nextTuple() {
		Utils.sleep(1);
		String data=null;
		try {			
			if((data=DataClient.dataQueue.poll(100, TimeUnit.MILLISECONDS))!=null){	
		//System.out.println(data);
				String[] columns=data.split(",");
				String token=columns[0];				
				this.fields=xml.getColumnNames(Integer.parseInt(token));
				if (fields!=null && columns.length == fields.size()){
					if (Integer.parseInt(token)<2500) {  //这是家电因素
						try {
							toRedis(columns);
						} catch (JSONException e) {
							e.printStackTrace();
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					try {
						long date = Long.parseLong( columns[1])/1000;
						if(Integer.parseInt(token)>2500){
							jedis.hset("sensorData:"+columns[2], columns[3]+"", date+"");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				String event="";
				if(columns[7].equals("501")){
					event="opened";
				}else if(columns[7].equals("502")){
				    event="closed";
				}else{
					event=columns[7];
				}
				
				switch (Integer.parseInt(token)) {   //factorID
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
				case 201:  //烟雾探测器
					log.debug("ctrolID:"+columns[2]+",roomID:"+columns[5]+","+"烟雾探测器 "+event);
					break;
				case 211:  //漏水探测器
					log.debug("ctrolID:"+columns[2]+",roomID:"+columns[5]+","+"漏水探测器 "+event);
					break;	
				case 221:  //漏水探测器
					log.debug("ctrolID:"+columns[2]+",roomID:"+columns[5]+","+"门磁 "+event);
					break;
				case 401:  //无线灯
					log.debug("ctrolID:"+columns[2]+",roomID:"+columns[5]+","+"无线灯 "+event);
					break;
				case 411:  //遥控窗
					log.debug("ctrolID:"+columns[2]+",roomID:"+columns[5]+","+"遥控窗 "+event);
					break;	
				case 541:  //空调				
					String air=columns[8]+","+columns[9]+","+columns[10]+","+columns[11]+","+columns[12]+","+columns[13]+","+columns[14]+","+columns[15];	
					columns=new String[]{columns[0],columns[1],columns[2],columns[3],columns[4],columns[5],columns[6],columns[7],air};
					break;
				default:
					break;
				}				
					//toMongoDB(fields,columns);
				if (columns.length==9) {
					_collector.emit(new Values((Object[])columns ));
				}else{
					log.error("column size mismatch:"+columns);
				}
					
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
		//declarer.declare(new Fields(this.fields));
	}
	
	public void toMongoDB(List<String> fields,String[] columns){
		if(this.mongo==null ||table==null ){
			return;
		}
		DBObject record = new BasicDBObject();
		for (int i = 0; i < columns.length; i++) {
			record.put(fields.get(i), columns[i]);			
		}
		table.insert(record);	
	}
	
	public void toRedis(String[] columns) throws JSONException, ParseException{
		DateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
		DateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int factorID=Integer.parseInt(columns[0]);
		int ctrolID= Integer.parseInt(columns[2]);
		int deviceID= Integer.parseInt(columns[3]);
		int onOff;
		JSONObject json;
		switch (factorID) {
		case 541:  //空调
			onOff=Integer.parseInt(columns[7]);
			int mode=Integer.parseInt(columns[8]);
			int speed=Integer.parseInt(columns[9]);
			int direction=Integer.parseInt(columns[10]);
			int temperature=Integer.parseInt(columns[11]);
			int key=Integer.parseInt(columns[12]);
			DeviceState state =new DeviceState(onOff, mode, speed, direction, temperature, key,0);
			json =new JSONObject();

			json.put("state",state);
			json.put("sender",0);     //指令来自中控
			json.put("time",sdf2.format(sdf.parse(columns[1])));
			this.jedis.hset(LogicControl.currentDeviceState+ctrolID, deviceID+"", json.toString());
		default:  //电视 或者其他家电
			onOff=Integer.parseInt(columns[7]);
			if(onOff==501 || onOff==502 ||onOff==1 ||onOff==0){  //开关指令
				json=new JSONObject();
				json.put("keyType",onOff);
				json.put("sender",0);     //指令来自中控
				json.put("time",sdf2.format(sdf.parse(columns[1])));
				this.jedis.hset(LogicControl.currentDeviceState+ctrolID, deviceID+"", json.toString());
			}
			break;
		}
	}
	
	public static void main(String[] args) {
		
	}
	
}
