package utilities;

import Entity.CityTemperatureMisurements;
import Entity.WeatherForecast;
import com.mapbox.api.geocoding.v5.GeocodingCriteria;
import com.mapbox.api.geocoding.v5.MapboxGeocoding;
import com.mapbox.geojson.Point;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ForecastParser {

    private static Map<String, String> cityNationMap = new HashMap<>();
    private static Map<String, Tuple2<Double, Double>> latLongMap= new HashMap<>();
    private static String[] citiesList;
    private static Pattern COMMA = Pattern.compile(",");

    public static void parseLatLong(List<String> latLongList){

        for(String a : latLongList){
            String[] words = COMMA.split(a);
            String country = "";

            MapboxGeocoding reverseGeocode = MapboxGeocoding.builder()
                    .accessToken("pk.eyJ1IjoiYWxlc3Npb3ZudCIsImEiOiJjanZvMW12OTQxdTBuNGFvaTN6MzV6ejQ0In0.P2obg0FgZYLsEscjbyUv5A")
                    .query(Point.fromLngLat(Double.parseDouble(words[2]), Double.parseDouble(words[1])))
                    .geocodingTypes(GeocodingCriteria.TYPE_COUNTRY)
                    .build();
            try {
                country = reverseGeocode.executeCall().body().features().get(0).placeName();
            } catch (IOException e) {e.printStackTrace();}
            cityNationMap.put(words[0], country);
            Tuple2<Double, Double> latLong = new Tuple2<Double, Double>(Double.parseDouble(words[1]), Double.parseDouble(words[2]));
            latLongMap.put(words[0], latLong);
        }
    }

    public static WeatherForecast[] parseForecast(String line){

        String[] words = COMMA.split(line);

        if(words[0].equals("datetime")){
            citiesList = new String[words.length-1];
            System.arraycopy(words, 1, citiesList, 0, words.length - 1);
            return null;
        }
        else {
            WeatherForecast[] weatherForecasts = new WeatherForecast[words.length - 1];
            for(int i = 1; i < words.length ; i++){
                if (words[i].equals("")){
                    words[i] = "no info";
                }
                weatherForecasts[i-1] = new WeatherForecast(words[0], citiesList[i-1], words[i] );
            }
            return weatherForecasts;
        }
    }

    public static List<CityTemperatureMisurements> parseForecastTemperature(String line){

        String[] words = COMMA.split(line);

        if(words[0].equals("datetime")){
            citiesList = new String[words.length-1];
            System.arraycopy(words, 1, citiesList, 0, words.length - 1);
            return null;
        }
        else {
            List<CityTemperatureMisurements> cityTemperatureMisurements = new ArrayList<>();
            for(int i = 1; i < words.length ; i++){
                if (!words[i].equals("")) cityTemperatureMisurements.add(new CityTemperatureMisurements(words[0], citiesList[i-1], Double.parseDouble(words[i]) ));
            }
            return cityTemperatureMisurements;
        }
    }

    public static String getNation(String city){
        return cityNationMap.get(city);
    }

    public static Tuple2<Double, Double> getLatLong(String city){ return latLongMap.get(city); }

    public static CityTemperatureMisurements convertMeasurement(CityTemperatureMisurements toConvert){

        toConvert.convertDate(getLatLong(toConvert.getCity())._1, getLatLong(toConvert.getCity())._2);
        return toConvert;

    }
}
