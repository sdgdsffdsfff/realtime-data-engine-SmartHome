package cooxm.spout;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import cooxm.bolt.ReactBolt;
import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.device.Warn;
import cooxm.devicecontrol.socket.Message;
import cooxm.util.RedisUtil;
import cooxm.weather.Weather;
import cooxm.weather.WeatherUtil;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Aug 4, 2015 6:55:16 PM 
 */

public class EverydayTask {
	static long cookieNo=((System.currentTimeMillis()/1000)%(24*3600))*10000;
	public static  BlockingQueue<String> dataQueue= new ArrayBlockingQueue<String>(50000);
    
    public static void updateWeather() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
            	DateFormat sdf3=new SimpleDateFormat("yyyyMMddHHmmssSSS");
            	WeatherUtil wu=new WeatherUtil();
            	try {
					Map<String,Weather> weatherMap=wu.getWeatherMap();
					for (Entry<String,Weather> entry: weatherMap.entrySet()) {
						if (entry.getValue().isBigRainy()) {  //下雨
							
							String s=3041+","+sdf3.format(new Date())+","+entry.getKey()+","+0+","+0+","+0+","+0+","+501+","+0;
							dataQueue.offer(s, 10, TimeUnit.MILLISECONDS);
						}
					}
				} catch (UnsupportedEncodingException | JSONException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
        };        
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);//每天       
        calendar.set(year, month, day, 8, 0, 00); //定制每天的09:00:00执行，
        Date date = calendar.getTime();
        Timer timer = new Timer();
               
        int period = 12 * 60 * 60 * 1000; //每天的date时刻执行task，每隔24小时重复执行        
        timer.schedule(task, date, period);        
        //timer.schedule(task, date); //每天的date时刻执行task, 仅执行一次
    }
    
    
    public static void findFaultSensor() {
    	Jedis jedis=RedisUtil.getJedis();
    	jedis.select(9);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
            	Date date=new Date(0);
            	int deviceID=-1;
            	DateFormat sdf3=new SimpleDateFormat("yyyyMMddHHmmss");
            	Set<String> keys = jedis.hkeys("sensorData:*");
            	for (String key : keys) {
            		String[] keycell=key.split(":");
            		Map<String, String> map = jedis.hgetAll(key);
            		try {
                		for (Entry<String, String> entry : map.entrySet()) {
    						String dateStr=entry.getValue();
    						 Date date1=sdf3.parse(dateStr);
    						 date=date1.after(date)?date1:date;					
    					}
    					long timeDiff=(new Date().getTime()-date.getTime())/1000;
    					if (timeDiff>=600) {    //4002中控下线
    						Warn warn =new Warn(Integer.parseInt(keycell[1]), 3, 3, 0, new Date(), 0, 4002, 0, Integer.parseInt(keycell[1]), "");
    						JSONObject json=new JSONObject();
    						json.put("ctrolID", Integer.parseInt(keycell[1]));
    						json.put("sender",5);
    						json.put("receiver",0); 
    						json.put("warn", warn.toJsonObject());
    						Message msg=new Message((short) (LogicControl.WARNING_START+3), cookieNo++ +"",json );
    						msg.writeBytesToSock2(ReactBolt.deviceControlServer.sock);
    					}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}

            }
        };      

        Timer timer = new Timer();       
        int period =  5* 60 * 1000; //每天的date时刻执行task，每隔300秒重复执行        
        timer.schedule(task, 0, period);        
        //timer.schedule(task, date); //每天的date时刻执行task, 仅执行一次
    }
    
    
    
    
    
    
    

    public static void main(String[] args) {
    	updateWeather();
    }
}

