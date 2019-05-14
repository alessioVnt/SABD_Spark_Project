import Entity.CityTemperatureMisurements;
import Entity.WeatherForecast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utilities.ForecastParser;

import java.util.Arrays;

public class Query3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read from the csv
        JavaRDD<String> rawData = sc.textFile("data/temperature.csv");

        Long startTime = System.nanoTime();

        //Getting header line and parse for city name
        JavaRDD<String> headerRDD = rawData.filter(line -> line.contains("datetime"));
        String header = headerRDD.collect().get(0);
        ForecastParser.parseForecast(header);

        JavaRDD<CityTemperatureMisurements> cityTemperatureMisurementsJavaRDD = rawData.subtract(headerRDD).flatMap(line -> Arrays.asList(ForecastParser.parseForecastTemperature(line)).iterator())
                .filter(y -> y.getYear() == 2016 || y.getYear() == 2017)
                .filter(w -> w.getMonth() != 5 && w.getMonth() != 10 && w.getMonth() != 11 && w.getMonth() != 12)
                .filter(h -> h.getHour() >= 12 && h.getHour() <= 15);

        //<k, v> -> <data + hour, <temp, occur>>
        JavaPairRDD<String, Tuple2> hourTemperature = cityTemperatureMisurementsJavaRDD.mapToPair(w -> new Tuple2<>(w.getCity() + w.getDateString(), new Tuple2<>(w.getTemperature(), 1)));


    }
}
