package cooxm.spout;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import cooxm.devicecontrol.control.Configure;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jun 30, 2015 11:41:20 AM 
 */

public class RedisThread implements Runnable {
	Jedis jedis;
	JedisPubSub jedisPubSub;
	public static  BlockingQueue<String> dataQueue= new ArrayBlockingQueue<String>(1000);
	
	RedisThread(){
		Configure cf=new Configure();
		String redis_ip         =cf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(cf.getValue("redis_port"));
		this.jedis= new Jedis(redis_ip, redis_port,10000);
		jedis.select(9);
        jedisPubSub=new JedisPubSub() {			
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
					//dataQueue.put(arg1);
					DataClient.dataQueue.put(arg1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
	}
	public void test() {
		jedis.subscribe(jedisPubSub, "remoteControlOperation");
		//System.out.print("hello world!");
	}
	



	@Override
	public void run() {
		jedis.subscribe(jedisPubSub, "profileOperation");	
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		RedisThread r=new RedisThread();
		r.test();

	}

}
