package bolt;

import java.io.IOException;
import java.net.Socket;
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
import cooxm.devicecontrol.socket.CtrolSocketServer;
import cooxm.devicecontrol.socket.Header;
import cooxm.devicecontrol.socket.Message;
import cooxm.devicecontrol.socket.SocketClient;
import cooxm.devicecontrol.util.MySqlClass;
import redis.clients.jedis.Jedis;
import trigger.RunTimeTrigger;
import trigger.RuntimeTriggerTemplateReact;
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
	private static SocketClient deviceControlServer=null;
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
		
		this.IP=config.getValue("device_server_IP");//config.getProperty("device_server_IP", "172.16.35.173");
		this.port =Integer.parseInt(config.getProperty("device_server_port","20190"));
		this.clusterID=Integer.parseInt(config.getValue("cluster_id"));
		this.serverID=Integer.parseInt(config.getValue("server_id"));
		this.serverType=200;	
         
		
		deviceControlServer= new SocketClient(this.IP,this.port,this.clusterID,this.serverID,this.serverType ,false);
		new Thread( this.deviceControlServer).start();
		//log.info("Successfull connect to msg Server: "+this.IP+":"+this.port);
		//deviceControlServer.sendAuth(1,5,200);	
//		Message msg=CtrolSocketServer.readFromClient(deviceControlServer.sock);
//		System.out.println(msg.toString());
//		if(msg!=null && msg.getJson()!=null && msg.getJson().optInt("errorCode")==0){
//			log.warn("\n#------------    Success connect to "+this.IP+":" +this.port +"         -------------------#\n");
//		}else{
//			log.warn("\n#------------    Failed to connect  "+this.IP+":" +this.port +"         -------------------#\n");
//		}
	}

	@Override
	public void execute(Tuple input) {
		if(deviceControlServer==null){
			deviceControlServer= new SocketClient(this.IP,this.port,this.clusterID,this.serverID,this.serverType,false );
			deviceControlServer.sendAuth(this.clusterID,this.serverID,this.serverType);	
		}

		List<Object> line=input.getValues();
		int ctrolID=(Integer)line.get(0);
		int roomID=(Integer)line.get(1);
		int triggerid=(Integer)line.get(2);
		TriggerTemplate tg = MatchingBolt2.triggerMap.get(triggerid);
		if(tg==null){
			log.error("can't get trigger in triggerMap,triggerID="+triggerid);
			return;
		}		
		List<TriggerTemplateReact> templateReact=tg.getTriggerTemplateReactList();
		if(templateReact==null){
			log.error("Can't get Triiger ReactList for triggerID:"+triggerid);
			return;
		}
		for(TriggerTemplateReact react: templateReact){
			RuntimeTriggerTemplateReact rReact=new RuntimeTriggerTemplateReact(react);
			Message msg=rReact.react(this.mysql, jedis, ctrolID, roomID);
			System.out.println("ReactBolt: Congrats!! rule has been triggerd,ctrolID="+ctrolID+",TriggerID="+triggerid+",command="+Integer.toHexString(msg.getCommandID())+"\n");
			if(msg!=null && deviceControlServer.sock!=null){
				msg.writeBytesToSock2(deviceControlServer.sock);
			}
			
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
		SocketClient sock=new SocketClient("172.16.35.173",20190,1,5,200,false);
		//sock.sendAuth(1,5,200);
		new Thread((Runnable) sock).start();
		Message a=CtrolSocketServer.readFromClient(sock.sock);
		System.out.println(a);
//		try {
//			Thread.sleep(1000*1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		

	}
}
