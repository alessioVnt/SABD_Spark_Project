package Entity;

import java.io.Serializable;

public class Query1Out implements Serializable {

    private String year;
    private String city;

    public Query1Out(String year, String city) {
        this.year = year;
        this.city = city;
    }

    public String getYear() {
        return year;
    }

    public String getCity() {
        return city;
    }
}
