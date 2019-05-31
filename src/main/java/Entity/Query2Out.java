package Entity;

import java.io.Serializable;

public class Query2Out implements Serializable {

    private String country;
    private String year;
    private String month;
    private String measurementType;
    private double mean;
    private double stdDev;
    private double max;
    private double min;


    public Query2Out(String country, String year, String month, String measurementType, double mean, double stdDev, double max, double min) {
        this.country = country;
        this.year = year;
        this.month = month;
        this.measurementType = measurementType;
        this.mean = mean;
        this.stdDev = stdDev;
        this.max = max;
        this.min = min;
    }

    public String getCountry() {
        return country;
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getMeasurementType() {
        return measurementType;
    }

    public double getMean() {
        return mean;
    }

    public double getStdDev() {
        return stdDev;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }
}
