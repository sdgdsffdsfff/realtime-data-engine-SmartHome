package bolt;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import cooxm.devicecontrol.util.MySqlClass;
import spout.DataClient;
import util.SystemConfig;
import Trigger.RunTimeTrigger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：5 Feb 2015 14:46:46 
 */

public class TriggerBolt implements IRichBolt{
	private Socket deviceControlServer=null;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		SystemConfig config= SystemConfig.getConf();
		this.deviceControlServer=config.getDeviceControlServer();
	}

	@Override
	public void execute(Tuple input) {
		List<Object> line=input.getValues();
		int controlID=(Integer)line.get(0);
		RunTimeTrigger trigger=(RunTimeTrigger)line.get(0);
		
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
	
	
	public static void main(String[] args) {
		try {
			Socket socket=new Socket("172.16.35.67", 20190);
			Thread.sleep(10000);
			socket.getOutputStream().write(new Byte("12345678901234567890123"));
			socket.getOutputStream().flush();
			Thread.sleep(100000);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
