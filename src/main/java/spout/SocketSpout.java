package spout;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
	private DataClient sock;
	//private Config config;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		sock=config.getDataClient();
		
		/*Config config=new Config();
		String IP=config.getValue("msg_server_ip");
		int port=Integer.parseInt(config.getValue("msg_server_port") );		
		try {
            sock=new DataClient(IP,port); 
            sock.toQueue();  
          } catch (UnknownHostException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }*/
	}
	
	@Override
	public void nextTuple() {
		String data=null;
		try {
			if((data=DataClient.dataQueue.poll(1000, TimeUnit.MILLISECONDS))!=null){
				String token=data.substring(0, data.indexOf(','));
				_collector.emit(new Values(token,data));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields("token","dataString"));
	}
	
	public static void main(String[] args) {
		MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");
	}
	
}
