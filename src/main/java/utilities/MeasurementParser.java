package utilities;

import com.mapbox.api.geocoding.v5.GeocodingCriteria;
import com.mapbox.api.geocoding.v5.MapboxGeocoding;
import com.mapbox.geojson.Point;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import java.io.IOException;
import org.joda.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

public class MeasurementParser {

    private static Map<String, String> cityNationMap = new HashMap<>();
    private static Pattern COMMA = Pattern.compile(",");
    private static String[] citiesList;

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
        }
    }

    public static void parseHeader(String line){

        String[] words = COMMA.split(line);
        citiesList = new String[words.length-1];
        //TODO: controlla se funziona questo tipo di copia dell'array
        System.arraycopy(words, 1, citiesList, 0, words.length - 1);
    }

    public static List<Tuple2<String, Double>> parseMeasurement(String line, String type){
        String[] words = COMMA.split(line);

        List<Tuple2<String, Double>> measurements = new ArrayList<>();
        for(int i = 1; i < words.length ; i++){
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String dateString = LocalDateTime.parse(words[0], dateTimeFormatter).toString().substring(0,7);
            String key = cityNationMap.get(citiesList[i-1]) + dateString;
            Double value;
            if(!words[i].equals("")){
                value = Double.parseDouble(words[i]);
                if(type.equals("TEMP") && value > 350){
                    continue;
                }
            }
            else{
                continue;
            }

            Tuple2<String,Double> measurement = new Tuple2<>(key, value);
            measurements.add(measurement);
        }
        return measurements;
    }
}
