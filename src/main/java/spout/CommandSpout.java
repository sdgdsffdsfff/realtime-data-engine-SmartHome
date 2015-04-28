package spout;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.socket.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.util.JedisByteHashMap;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 21, 2015 10:45:42 AM 
 */

public class CommandSpout extends BaseRichSpout {
	Jedis jedis;
	ArrayBlockingQueue<String> inCommand=new ArrayBlockingQueue<String>(1000);

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		this.jedis= new Jedis(redis_ip, redis_port,200);
        JedisPubSub jedisPubSub=new JedisPubSub() {
			
			@Override
			public void onUnsubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub				
			}
			
			@Override
			public void onSubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onPUnsubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onPSubscribe(String arg0, int arg1) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void onPMessage(String arg0, String arg1, String arg2) {
				// TODO Auto-generated method stub				
			}
			
			@Override
			public void onMessage(String arg0, String arg1) {
				//System.out.println(arg0+ "_"+ arg1);
				try {
					inCommand.offer(arg1, 200, TimeUnit.MICROSECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		};
		jedis.subscribe(jedisPubSub, "commandQueue");
	}

	@Override
	public void nextTuple() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
