package Entity;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import utilities.TimeZoneConverter;

import java.io.Serializable;

public class CityTemperatureMisurements implements Serializable {
    private LocalDateTime date;
    private String city;
    private Double temperature;

    public CityTemperatureMisurements(String dateStr, String city, Double temperature) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        this.date = LocalDateTime.parse(dateStr, dateTimeFormatter);
        this.city = city;
        this.temperature = temperature;
    }

    public String getDateString(){
        String stringDate = this.date.toString().substring(0, 10);
        return stringDate;
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public int getMonth(){
        return this.date.getMonthOfYear();
    }

    public int getHour(){
        return this.date.getHourOfDay();
    }

    public int getYear(){
        return this.date.getYear();
    }

    public void convertDate(double latitude, double longitude){
        this.date = TimeZoneConverter.convertTimeZone(this.date, (long)latitude, (long)longitude);
    }
}
