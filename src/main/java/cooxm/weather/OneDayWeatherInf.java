package cooxm.weather;
/** 
 * @author Chen Guanghua E-mail: richard@cooxm.com
 * @version Created：Jul 29, 2015 4:09:05 PM 
 */

//OneDayWeatherInf是自己定义的承载某一天的天气信息的实体类  
public class OneDayWeatherInf {  
  
    private  
        String location;  
        String date;  
        String week;  
        String tempertureOfDay;  
        String tempertureNow;  
        String wind;  
        String weather;   
        String picture;  
        int pmTwoPointFive;  
  
    public OneDayWeatherInf(){  
          
        location = "";  
        date = "";  
        week = "";  
        tempertureOfDay = "";  
        tempertureNow = "";  
        wind = "";  
        weather = "";  
        picture = "undefined";  
        pmTwoPointFive = 0;  
    }  
          
          
          
    public String getLocation() {  
        return location;  
    }  
    public void setLocation(String location) {  
        this.location = location;  
    }  
    public String getDate() {  
        return date;  
    }  
    public void setDate(String date) {  
        this.date = date;  
    }  
    public String getWeek() {  
        return week;  
    }  
    public void setWeek(String week) {  
        this.week = week;  
    }  
    public String getTempertureOfDay() {  
        return tempertureOfDay;  
    }  
    public void setTempertureOfDay(String tempertureOfDay) {  
        this.tempertureOfDay = tempertureOfDay;  
    }  
    public String getTempertureNow() {  
        return tempertureNow;  
    }  
    public void setTempertureNow(String tempertureNow) {  
        this.tempertureNow = tempertureNow;  
    }  
    public String getWind() {  
        return wind;  
    }  
    public void setWind(String wind) {  
        this.wind = wind;  
    }  
    public String getWeather() {  
        return weather;  
    }  
    public void setWeather(String weather) {  
        this.weather = weather;  
    }  
    public String getPicture() {  
        return picture;  
    }  
    public void setPicture(String picture) {  
        this.picture = picture;  
    }  
    public int getPmTwoPointFive() {  
        return pmTwoPointFive;  
    }  
    public void setPmTwoPointFive(int pmTwoPointFive) {  
        this.pmTwoPointFive = pmTwoPointFive;  
    }  
  
    public String toString(){  
          
        return location+"   "+date+"   "+week+" tempertureOfDay：  "+tempertureOfDay+" tempertureNow：  "+tempertureNow+"   "+wind+"   "+weather+"   "+picture+"   "+pmTwoPointFive;  
    }  
      
      
}  