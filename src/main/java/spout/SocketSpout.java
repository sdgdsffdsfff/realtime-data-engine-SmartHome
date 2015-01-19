package spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created£º4 Jan 2015 14:46:33 
 */

public class SocketSpout  extends BaseRichSpout {

	private SpoutOutputCollector _collector;
	private BufferedReader br;
	DataClient sock;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;		
		try {
            sock=new DataClient("192.168.27.100",5678);
            br= new BufferedReader(new InputStreamReader(sock.getInputStream()));   
          } catch (UnknownHostException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
	}
	
	@Override
	public void nextTuple() {
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
		declarer.declare(new Fields("test"));
	}


}
