package cooxm.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import redis.clients.jedis.Jedis;
import cooxm.devicecontrol.control.Configure;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.spout.DataClient;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：2 Feb 2015 17:14:23 
 */

public class SystemConfig extends Configure {

	private static final long serialVersionUID = 1L;
	private static  SystemConfig config = new SystemConfig();
	static Configure conf;
	private static MySqlClass mysql;	
	private static DataClient dataClient;

	public static  SystemConfig getConf() {
		return config;
	}

	public  MySqlClass getMysql() {
		String mysql_ip			=conf.getValue("mysql_ip");
		String mysql_port		=conf.getValue("mysql_port");
		String mysql_user		=conf.getValue("mysql_user");
		String mysql_password	=conf.getValue("mysql_password");
		String mysql_database	=conf.getValue("mysql_database");		
		mysql=new MySqlClass(mysql_ip, mysql_port, mysql_database, mysql_user, mysql_password);
		
		return mysql;
	}
	public  DataClient getDataClient() {
		
		String data_server_IP=conf.getValue("data_server_ip");
		int data_server_port =Integer.parseInt(conf.getValue("data_server_port"));
			try {
				dataClient=new DataClient(data_server_IP,data_server_port);
			} catch (IOException e) {
				e.printStackTrace();
			}	
		
		return dataClient;
	}
	
	public Socket getDeviceControlServer(){
		Socket socket=null;
		String device_server_IP=conf.getProperty("device_server_IP", "172.16.35.173");
		int device_server_port =Integer.parseInt(conf.getProperty("server_port","20190"));
		InetAddress remoteaddress=null;
		InetAddress localaddress=null;
		try {
			remoteaddress = InetAddress.getByName(device_server_IP);
			localaddress=InetAddress.getByName("0.0.0.0");
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		return socket;
	}

	private SystemConfig() {
		this.conf =new Configure();
	}
	
	public int getTriggerTimeOut(){
		return Integer.parseInt(conf.getProperty("trigger_time_out", "3"));
	}
	
	public Jedis getJedis(){
		String redis_ip         =conf.getValue("redis_ip");
		int redis_port       	=Integer.parseInt(conf.getValue("redis_port"));	
		Jedis jedis=new  Jedis(redis_ip, redis_port,10000);
		jedis.select(9);
		//jedis.configSet("timeout", "4000");
		return jedis;
	}
	

	
	public static void main(String[] args) {
		SystemConfig confg=SystemConfig.getConf();
		MySqlClass mysql=confg.getMysql();
		System.out.println(mysql.select("show tables;"));
		DataClient sock=confg.getDataClient();
		
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		//MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "cooxm", "cooxm");
	}

}
