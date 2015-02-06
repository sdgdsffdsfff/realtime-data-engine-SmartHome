package util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import spout.DataClient;
import cooxm.devicecontrol.control.Config;
import cooxm.devicecontrol.util.MySqlClass;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：2 Feb 2015 17:14:23 
 */

public class SystemConfig extends Config {

	private static final long serialVersionUID = 1L;
	private static  SystemConfig config = new SystemConfig();
	static Config conf;
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
		InetAddress remoteaddress=null;
		InetAddress localaddress=null;
		try {
			remoteaddress = InetAddress.getByName(data_server_IP);
			localaddress=InetAddress.getByName("0.0.0.0");
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		};

		if(DataClient.isReachable(localaddress, remoteaddress, data_server_port, 5000)){
			try {
				dataClient=new DataClient(data_server_IP,data_server_port);
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		
		return dataClient;
	}
	
	public Socket getDeviceControlServer(){
		Socket socket=null;
		String device_server_IP=conf.getProperty("device_server_IP", "172.16.35.173");
		int device_server_port =Integer.parseInt(conf.getProperty("server_port","20190"));
		try {
			 socket=new Socket(device_server_IP, device_server_port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return socket;
	}

	private SystemConfig() {
		this.conf =new Config();
	}
	
	public int getTriggerTimeOut(){
		return Integer.parseInt(conf.getProperty("trigger_time_out", "3"));
	}
	
	
	public static void main(String[] args) {
		SystemConfig confg=SystemConfig.getConf();
		MySqlClass mysql=confg.getMysql();
		System.out.println(mysql.select("show tables;"));
		Socket sock=confg.getDataClient();
		
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//MySqlClass mysql=new MySqlClass("172.16.35.170","3306","cooxm_device_control", "root", "cooxm");


	}

}
