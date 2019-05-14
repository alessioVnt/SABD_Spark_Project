package utilities;

import Entity.WeatherForecast;

import java.util.List;
import java.util.regex.Pattern;

public class ForecastParser {

    private static String[] citiesList;
    private static Pattern COMMA = Pattern.compile(",");

    public static WeatherForecast[] parseForecast(String line){

        String[] words = COMMA.split(line);

        if(words[0].equals("datetime")){
            citiesList = new String[words.length-1];
            for(int i=1;i<words.length; i++){
                citiesList[i-1] = words[i];
            }
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
}
