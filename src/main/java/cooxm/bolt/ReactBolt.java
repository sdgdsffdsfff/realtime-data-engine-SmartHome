package cooxm.bolt;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import cooxm.devicecontrol.device.TriggerTemplate;
import cooxm.devicecontrol.device.TriggerTemplateReact;
import cooxm.devicecontrol.socket.CtrolSocketServer;
import cooxm.devicecontrol.socket.Message;
import cooxm.devicecontrol.socket.SocketClient;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.trigger.RuntimeTriggerTemplateReact;
import cooxm.util.SystemConfig;
import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：5 Feb 2015 14:46:46 
 */

public class ReactBolt implements IRichBolt{
	OutputCollector _collector;
	static Logger log =Logger.getLogger(ReactBolt.class);
	private Jedis jedis;
	MySqlClass mysql;
	public static SocketClient deviceControlServer=null;
	String IP;
	int port;
	int clusterID;
	int targetServerID;
	int serverID;
	int serverType;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config=SystemConfig.getConf();	
		this.jedis=config.getJedis();
		this.jedis.select(9);
		this.mysql=config.getMysql();
		
		this.IP=config.getValue("device_server_IP");//config.getProperty("device_server_IP", "172.16.35.173");
		this.port =Integer.parseInt(config.getProperty("device_server_port","20190"));
		this.clusterID=Integer.parseInt(config.getValue("cluster_id"));
		this.serverID=Integer.parseInt(config.getValue("server_id"));
		this.serverType=Integer.parseInt(config.getValue("server_type"));
		this.targetServerID=4;
         
		
		deviceControlServer= new SocketClient(this.IP,this.port,this.clusterID,this.targetServerID,this.serverID,this.serverType ,true,false);
		Thread controlServerThread = new Thread( this.deviceControlServer);
		controlServerThread.setName("controlServerThread");
		controlServerThread.start();
	}

	@Override
	public void execute(Tuple input) {

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
			Message msg=null;
			try {
				msg = rReact.react(this.mysql, jedis, ctrolID, roomID);
			} catch (ParseException e) {
				e.printStackTrace();
			}			
			if(msg!=null && deviceControlServer.sock!=null){
				log.info("command has been send,ctrolID="+ctrolID+",TriggerID="+triggerid
						+",triggerName="+tg.getTriggerName()+",commandID="+Integer.toHexString(msg.getCommandID())+",msg:"+msg.toString());
				try {
					msg.writeBytesToSock2(deviceControlServer.sock);
				} catch (Exception e) {
					e.printStackTrace();
					try {
						log.info("socket has been closed:"+deviceControlServer.sock.getRemoteSocketAddress());
						deviceControlServer.sock.getOutputStream().close();
						deviceControlServer.sock.close();
						deviceControlServer.sock=null;
						
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}			
		}
		//System.out.println("\n");
		this._collector.emit(new Values("react"));
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
		SocketClient sock=new SocketClient("172.16.35.173",20190,1,4,5,200,false,true);
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
