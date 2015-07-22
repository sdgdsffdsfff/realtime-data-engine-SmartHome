package cooxm.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.socket.Message;
import cooxm.util.PraseXmlUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.util.JedisByteHashMap;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Apr 21, 2015 10:45:42 AM 
 */

public class RedisSpout extends BaseRichSpout {

	static Logger log =Logger.getLogger(SocketSpout.class);
	private SpoutOutputCollector _collector;
	RedisThread rt;
	private PraseXmlUtil xml;
	List<String> fields=new ArrayList<String>();
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector=collector;
		this.xml=new PraseXmlUtil();
		
		rt=new RedisThread();
		new Thread(rt).start();
	}

	@Override
	public void nextTuple() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}


}
