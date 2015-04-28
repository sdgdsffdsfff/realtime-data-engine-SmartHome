package bolt;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.DeviceState;
import cooxm.devicecontrol.device.TriggerTemplate;
import cooxm.devicecontrol.device.TriggerTemplateMap;
import cooxm.devicecontrol.device.TriggerTemplateReact;
import cooxm.devicecontrol.device.Warn;
import cooxm.devicecontrol.socket.Header;
import cooxm.devicecontrol.socket.Message;
import cooxm.devicecontrol.socket.SocketClient;
import cooxm.devicecontrol.util.MySqlClass;
import redis.clients.jedis.Jedis;
import trigger.RunTimeTrigger;
import util.SystemConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：5 Feb 2015 14:46:46 
 */

public class ReactBolt implements IRichBolt{
	static Logger log =Logger.getLogger(ReactBolt.class);
	private Jedis jedis;
	MySqlClass mysql;
	private  SocketClient deviceControlServer=null;
	String IP;
	int port;
	int clusterID;
	int serverID;
	int serverType;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		SystemConfig config=SystemConfig.getConf();	
		this.jedis=config.getJedis();
		this.mysql=config.getMysql();
		
		this.IP=config.getProperty("device_server_IP", "172.16.35.173");
		this.port =Integer.parseInt(config.getProperty("server_port","20190"));
		this.clusterID=1;
		this.serverID=5;
		this.serverType=200;	
		
		try {
			this.deviceControlServer= new SocketClient(this.IP,this.port,this.clusterID,this.serverID,this.serverType );
			this.deviceControlServer.sendAuth(1,5,200);
			log.info("SocketSpout,connect to "+this.IP+":" +this.port +"success.");				
		} catch (IOException e) {
			log.error(e);
			try {
				log.error("SocketSpout,connect to "+this.IP+":" +this.port +"failed, socket will be close(). waiting for 30 to reconnect...");
				Thread.sleep(30*1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

	@Override
	public void execute(Tuple input) {
		if(this.deviceControlServer==null){
			try {
				this.deviceControlServer= new SocketClient(this.IP,this.port,this.clusterID,this.serverID,this.serverType );
				this.deviceControlServer.sendAuth(1,6,201);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		List<Object> line=input.getValues();
		int ctrolID=(Integer)line.get(0);
		int roomID=(Integer)line.get(1);
		int triggerid=(Integer)line.get(2);
		List<TriggerTemplateReact> templateReact=MatchingBolt2.triggerMap.get(triggerid).getTriggerTemplateReactList();
		for(TriggerTemplateReact react: templateReact){
			Message msg=react.react(this.mysql, jedis, ctrolID, roomID);
			msg.writeBytesToSock2(this.deviceControlServer);
			System.out.println("Congrats!! rule has been triggerd,ctrolID="+ctrolID+",TriggerID="+triggerid+",command="+Integer.toHexString(msg.getCommandID()));
		}		
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		SocketClient sock=new SocketClient("172.16.35.67",20190,1,5,200);
		sock.sendAuth(1,5,200);
		new Thread((Runnable) sock).start();
		
//		try {
//			Thread.sleep(1000*1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		

	}
}
