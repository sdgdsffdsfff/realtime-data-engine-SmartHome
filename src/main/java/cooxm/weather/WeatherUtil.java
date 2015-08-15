package cooxm.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import cooxm.devicecontrol.control.LogicControl;
import cooxm.devicecontrol.util.MySqlClass;
import cooxm.util.SystemConfig;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jun 25, 2015 5:15:56 PM 
 */

public class WeatherUtil {	

	private static  String httpUrl = "http://apis.baidu.com/apistore/weatherservice/cityname";
    
    MySqlClass mysql;
    
    public WeatherUtil(){
        SystemConfig configure =SystemConfig.getConf();
        this.mysql=configure.getMysql();
    }
    
    public Map<String,String> getCityMap() throws UnsupportedEncodingException{
    	Map<String,String> cityMap=new HashMap<String,String>();
    	String sql="select ctr_id, precityname from cooxm_main.info_controller_reg t1 "
    			+ "join cooxm_main.dic_areacode_detail t2 ON t1.areacode=t2.threelevelcode"
    			+ " where t2.level=3 ; ";
    	String res=this.mysql.select(sql);
    	String[] cells=res.split("\n");
    	if(cells==null || cells.length==0 ){
    		 return null;
    	}
    	for (int i = 0; i < cells.length; i++) {
    		 String[] cell=	cells[i].split(",");
    		 String cityName=cell[1];
 	    	if(cityName.contains("市")){
 	    		cityName=cityName.substring(0, cell[1].indexOf("市"));
	    	}
    		cityMap.put(cell[0], cityName);
		}
    	return cityMap;
    }
    
    public   Map<String,Weather> getWeatherMap() throws JSONException, UnsupportedEncodingException{
    	Map<String,Weather> weatherMap=new HashMap<String,Weather>();
    	Map<String,String> cityMap=getCityMap();
    	for (Entry<String,String> entry : cityMap.entrySet()) {
    		String cityNameURL=URLEncoder.encode(entry.getValue(),"utf-8");
    		String weatherStr=requestWeather(cityNameURL);
			if (weatherStr!=null) {
				JSONObject json=new JSONObject(weatherStr);
				int errorCode=json.getInt("errNum");
				if (errorCode==0) {
					JSONObject json2=json.getJSONObject("retData");
					Weather w=new Weather(json2);
					weatherMap.put(entry.getKey(), w);
				}				
			}
		}
    	return weatherMap;
    }
    
    

	public  String requestWeather( String cityChineseName) {
		httpUrl="http://apis.baidu.com/apistore/weatherservice/cityname";
	    BufferedReader reader = null;
	    String result = null;
	    StringBuffer sbf = new StringBuffer();
	    httpUrl = httpUrl + "?" + "cityname="+cityChineseName;

	    try {
	        URL url = new URL(httpUrl);
	        HttpURLConnection connection = (HttpURLConnection) url
	                .openConnection();
	        connection.setRequestMethod("GET");
	        // 填入apikey到HTTP header
	        connection.setRequestProperty("apikey",  "63b615c7a571dbd0fb698462ef7582b3");
	        connection.connect();
	        InputStream is = connection.getInputStream();
	        reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
	        String strRead = null;
	        while ((strRead = reader.readLine()) != null) {
	            sbf.append(strRead);
	            sbf.append("\r\n");
	        }
	        reader.close();
	        result = sbf.toString();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return result;
	}
	
	


	public static void main(String[] args) throws UnsupportedEncodingException, JSONException {
		
		Set<String> cityid=new HashSet<>();
		cityid.add("110100");cityid.add("130200");

		WeatherUtil wu=new WeatherUtil();
		String httpArg = "%E6%B7%B1%E5%9C%B3"; 
		String jsonResult = wu.requestWeather( httpArg);
		JSONObject json=new JSONObject(jsonResult);
		JSONObject json2=json.getJSONObject("retData");
		String city=json2.getString("city");
		System.out.println(json);
		
		Weather w=new Weather(json2);
		System.out.println(w.getWeather());
		boolean rain=w.isBigRainy();
		System.out.println(rain);
		
		
		Map<String, Weather> x = wu.getWeatherMap();
		
		System.out.println(x.size());
		

	}

}
