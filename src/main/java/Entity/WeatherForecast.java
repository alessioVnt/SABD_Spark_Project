package Entity;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.regex.Pattern;

public class WeatherForecast implements Serializable {
    private LocalDateTime date;
    private String city;
    private String weather;
    @Nullable
    private Integer clear;

    public WeatherForecast(String dateStr, String city, String weather) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        this.date = LocalDateTime.parse(dateStr, dateTimeFormatter);
        this.city = city;
        this.weather = weather;
    }

    public WeatherForecast(LocalDateTime date, String city, String weather) {
        this.date = date;
        this.city = city;
        this.weather = weather;
    }

    public String getCity() {
        return city;
    }

    public String getWeather() {
        return weather;
    }

    public LocalDateTime getDate(){
        return this.date;
    }

    public String getDateString(){
        String stringDate = this.date.toString().substring(0, 10);
        return stringDate;
    }

    public int getYear(){
        return this.date.getYear();
    }

    public int getMonth(){
        return this.date.getMonthOfYear();
    }

    public int getDay(){
        return this.date.getDayOfMonth();
    }

    public int getHour(){
        return this.date.getHourOfDay();
    }

    @Nullable
    public Integer getClear() {
        return clear;
    }

    public void setClear(@Nullable Integer clear) {
        this.clear = clear;
    }

    //For test purpose, erase before delivery
    public static void main(String[] args){
        Pattern COMMA = Pattern.compile(",");
        WeatherForecast weatherForecast = new WeatherForecast("2012-10-01 13:00:00", "Rome", "Sky is clear");
        String line = "2012-10-01 13:00:00,scattered clouds,light rain,sky is clear,mist,sky is clear,sky is clear,sky is clear,sky is clear,light rain,sky is clear,mist,sky is clear,sky is clear,broken clouds,sky is clear,overcast clouds,mist,overcast clouds,light rain,sky is clear,scattered clouds,mist,light intensity drizzle,mist,broken clouds,few clouds,sky is clear,sky is clear,sky is clear,haze,sky is clear,sky is clear,sky is clear\n";
        String[] words = COMMA.split(line);
        //System.out.println(words[1]);
        LocalDateTime localDateTime = weatherForecast.getDate();
        System.out.println(localDateTime.toString());
        localDateTime = localDateTime.withHourOfDay(0);
        System.out.println(localDateTime.toString());
        LocalDateTime localDateTime1 = LocalDateTime.parse(localDateTime.toString());
        //System.out.println(localDateTime.toString());
        //System.out.println(localDateTime1.toString());
        System.out.println(weatherForecast.getDateString());
    }
}
