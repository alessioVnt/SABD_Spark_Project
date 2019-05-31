package Entity;

import java.io.Serializable;

public class Query3Out implements Serializable {
    private String nation_city;
    private String year;
    private double temperatureGap;

    public Query3Out(String nation_city, String year, double temperatureGap) {
        this.nation_city = nation_city;
        this.year = year;
        this.temperatureGap = temperatureGap;
    }

    public String getNation_city() {
        return nation_city;
    }

    public String getYear() {
        return year;
    }

    public double getTemperatureGap() {
        return temperatureGap;
    }
}
