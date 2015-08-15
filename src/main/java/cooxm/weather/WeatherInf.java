package cooxm.weather;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jul 29, 2015 4:08:22 PM 
 */

//WeatherInf 是自己定义的承载所有的天气信息的实体类，包含了四天的天气信息。
public class WeatherInf {

	private OneDayWeatherInf[] weatherInfs;
	private String dressAdvise;//穿衣建议
	private String washCarAdvise;//洗车建议
	private String coldAdvise;//感冒建议
	private String sportsAdvise;//运动建议
	private String ultravioletRaysAdvise;//紫外线建议
	
	
	public WeatherInf(){
		dressAdvise = "";
		washCarAdvise = "";
		coldAdvise = "";
		sportsAdvise = "";
		ultravioletRaysAdvise = "";
	}
	
	public void printInf(){
		
		System.out.println(dressAdvise);
		System.out.println(washCarAdvise);
		System.out.println(coldAdvise);
		System.out.println(sportsAdvise);
		System.out.println(ultravioletRaysAdvise);
		for(int i=0;i<weatherInfs.length;i++){
			System.out.println(weatherInfs[i]);
		}
		
	}
	

	public OneDayWeatherInf[] getWeatherInfs() {
		return weatherInfs;
	}


	public void setWeatherInfs(OneDayWeatherInf[] weatherInfs) {
		this.weatherInfs = weatherInfs;
	}


	public String getDressAdvise() {
		return dressAdvise;
	}


	public void setDressAdvise(String dressAdvise) {
		this.dressAdvise = dressAdvise;
	}


	public String getWashCarAdvise() {
		return washCarAdvise;
	}


	public void setWashCarAdvise(String washCarAdvise) {
		this.washCarAdvise = washCarAdvise;
	}


	public String getColdAdvise() {
		return coldAdvise;
	}


	public void setColdAdvise(String coldAdvise) {
		this.coldAdvise = coldAdvise;
	}


	public String getSportsAdvise() {
		return sportsAdvise;
	}


	public void setSportsAdvise(String sportsAdvise) {
		this.sportsAdvise = sportsAdvise;
	}


	public String getUltravioletRaysAdvise() {
		return ultravioletRaysAdvise;
	}


	public void setUltravioletRaysAdvise(String ultravioletRaysAdvise) {
		this.ultravioletRaysAdvise = ultravioletRaysAdvise;
	}
	
}