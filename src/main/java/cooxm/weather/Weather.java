package cooxm.weather;

import org.json.JSONException;
import org.json.JSONObject;

/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jun 25, 2015 5:08:11 PM 
 */

public class Weather {
	private String   city;          // "北京", //城市
	private String   pinyin;        // "beijing", //城市拼音
	private String   citycode;      // "101010100",  //城市编码	
	private String   date;          // "15-02-11", //日期
	private String   time;          // "11;                       //00", //发布时间
	private String   postCode;      // "100000", //邮编
	private double      longitude;     // 116.391, //经度
	private double      latitude;      // 39.904, //维度
	private String   altitude;      // "33", //海拔	
	private String   weather;       // "晴",  //天气情况
	private String   temp;          // "10", //气温
	private String   l_tmp;         // "-4", //最低气温
	private String   h_tmp;         // "10", //最高气温
	private String   WD;            // "无持续风向",	 //风向
	private String   WS;            // "微风(<10m/h)", //风力
	private String   sunrise;       // "07:12", //日出时间
	private String   sunset;        // "17:44" //日落时间

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getPinyin() {
		return pinyin;
	}

	public void setPinyin(String pinyin) {
		this.pinyin = pinyin;
	}

	public String getCitycode() {
		return citycode;
	}

	public void setCitycode(String citycode) {
		this.citycode = citycode;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getPostCode() {
		return postCode;
	}

	public void setPostCode(String postCode) {
		this.postCode = postCode;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public String getAltitude() {
		return altitude;
	}

	public void setAltitude(String altitude) {
		this.altitude = altitude;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}

	public String getTemp() {
		return temp;
	}

	public void setTemp(String temp) {
		this.temp = temp;
	}

	public String getL_tmp() {
		return l_tmp;
	}

	public void setL_tmp(String l_tmp) {
		this.l_tmp = l_tmp;
	}

	public String getH_tmp() {
		return h_tmp;
	}

	public void setH_tmp(String h_tmp) {
		this.h_tmp = h_tmp;
	}

	public String getWD() {
		return WD;
	}

	public void setWD(String wD) {
		WD = wD;
	}

	public String getWS() {
		return WS;
	}

	public void setWS(String wS) {
		WS = wS;
	}

	public String getSunrise() {
		return sunrise;
	}

	public void setSunrise(String sunrise) {
		this.sunrise = sunrise;
	}

	public String getSunset() {
		return sunset;
	}

	public void setSunset(String sunset) {
		this.sunset = sunset;
	}

	public static void main(String[] args) {

	}

	public Weather(String city, String pinyin, String citycode,
			String date, String time, String postCode, double longitude,
			double latitude, String altitude, String weather, String temp,
			String l_tmp, String h_tmp, String wD, String wS, String sunrise,
			String sunset) {
		this.city = city;
		this.pinyin = pinyin;
		this.citycode = citycode;
		this.date = date;
		this.time = time;
		this.postCode = postCode;
		this.longitude = longitude;
		this.latitude = latitude;
		this.altitude = altitude;
		this.weather = weather;
		this.temp = temp;
		this.l_tmp = l_tmp;
		this.h_tmp = h_tmp;
		WD = wD;
		WS = wS;
		this.sunrise = sunrise;
		this.sunset = sunset;
	}
	
	public Weather(JSONObject json){

		try {
			this.city        =        json.getString("city");
			this.pinyin        =        json.getString("pinyin");
			this.citycode        =        json.getString("citycode");
			this.date        =        json.getString("date");
			this.time        =        json.getString("time");
			this.postCode        =        json.getString("postCode");
			this.longitude        =        json.getDouble("longitude");
			this.latitude        =        json.getDouble("latitude");
			this.altitude        =        json.getString("altitude");
			this.weather        =        json.getString("weather");
			this.temp        =        json.getString("temp");
			this.l_tmp        =        json.getString("l_tmp");
			this.h_tmp        =        json.getString("h_tmp");
			WD        =        json.getString("WD");
			WS        =        json.getString("WS");
			this.sunrise        =        json.getString("sunrise");
			this.sunset        =        json.getString("sunset");
		} catch (JSONException e) {
			e.printStackTrace();
		}

		
	}
	
	public JSONObject toJson(){
		JSONObject json=new JSONObject();
		try {
			json.put("city",city);
			json.put("pinyin",pinyin);
			json.put("citycode",citycode);
			json.put("date",date);
			json.put("time",time);
			json.put("postCode",postCode);
			json.put("longitude",longitude);
			json.put("latitude",latitude);
			json.put("altitude",altitude);
			json.put("weather",weather);
			json.put("temp",temp);
			json.put("l_tmp",l_tmp);
			json.put("h_tmp",h_tmp);
			json.put("WD",WD);
			json.put("WS",WS);
			json.put("sunrise",sunrise);
			json.put("sunset",sunset);
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return json;		
	}
	
	public boolean isBigRainy(){
		if(this.weather.contains("大雨") || this.weather.contains("暴雨")|| this.weather.contains("雷雨")
				|| this.weather.contains("阵雨") || this.weather.contains("中雨") ){
			return true;
		}else{
			return false;
		}
	}

}
