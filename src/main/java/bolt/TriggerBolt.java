package bolt;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created��4 Jan 2015 11:08:18 
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.SystemConfig;
import backtype.storm.tuple__init;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import cooxm.devicecontrol.control.Config;
import cooxm.devicecontrol.device.*;
import cooxm.devicecontrol.util.MySqlClass;

public class TriggerBolt  implements IRichBolt {
	
	OutputCollector _collector;
	private static TriggerMap triggerMap;
	
	public static void main(String[] args) {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector=collector;
		SystemConfig config= SystemConfig.getConf();
		triggerMap=new TriggerMap(config.getMysql());
		/*Config conf=new Config();
		String mysql_ip			=conf.getValue("mysql_ip");
		String mysql_port		=conf.getValue("mysql_port");
		String mysql_user		=conf.getValue("mysql_user");
		String mysql_password	=conf.getValue("mysql_password");
		String mysql_database	=conf.getValue("mysql_database");
		MySqlClass mysql=new MySqlClass(mysql_ip, mysql_port, mysql_database, mysql_user, mysql_password);
		triggerMap=new TriggerMap(mysql);*/
	}

	@Override
	public void execute(Tuple input) {
		List<Object> x = input.getValues();
		input.get
		
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

}
