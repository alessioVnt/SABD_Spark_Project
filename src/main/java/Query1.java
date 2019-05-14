

/*
Per ogni anno del dataset individuare le citta che hanno almeno 15 giorni al mese di tempo sereno nei `
mesi di marzo, aprile e maggio.
Nota: tempo sereno nei mesi di marzo, aprile e maggio da intendersi in AND. Determinare un criterio
per decidere se il giorno e sereno, considerando l’informazione oraria a disposizione.
*/

import Entity.WeatherForecast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.LocalDateTime;
import scala.Tuple2;
import utilities.ForecastParser;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Query1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read from the csv
        JavaRDD<String> rawForecast = sc.textFile("data/weather_description.csv");

        Long startTime = System.nanoTime();

        //Getting header line to get cities list
        JavaRDD<String> headerRDD = rawForecast.filter(line -> line.contains("datetime"));
        String header = headerRDD.collect().get(0);
        ForecastParser.parseForecast(header);

        //Remove header line and flatmap to create WeatherForecast entities
        JavaRDD<WeatherForecast> weatherForecasts = rawForecast.subtract(headerRDD)
                .flatMap(line -> Arrays.asList(ForecastParser.parseForecast(line)).iterator())
                .filter(w -> w.getMonth() >= 3 && w.getMonth() <= 5);

        //Getting years list
        //TO REMOVE IF NOT NEEDED
        /*JavaRDD<String> years = weatherForecasts.flatMap(wf ->Arrays.asList(Integer.toString(wf.getYear())).iterator()).distinct();
        System.out.println(years.count());*/

        //Creates couple <key,value>, the key is city+date, the value is the attribute clear
        JavaPairRDD<String, Integer> hourWeather = weatherForecasts.mapToPair(w -> new Tuple2<>(w.getCity() + w.getDateString(), w.getHour() >= 7 && w.getHour() <= 20 && w.getWeather().equals("sky is clear") ? 1 : 0));

        //First reduce round, the new value is 1 if 80% of time was "sky is clear" (hour->day)
        //Creation of the new couple key value -> the new key doesen't contain the day anymore, the value is 1 if the day is considered a clear day
        //Filter to let pass only the clear sky days
        JavaPairRDD<String, Integer> dayWeather = hourWeather.reduceByKey((x, y) -> x + y)
                .mapToPair(h -> new Tuple2<>(h._1.substring(0, h._1.length() - 3), h._2 >= 10 ? 1 : 0))
                .filter(w -> w._2 == 1);

        //Second reduce round to obtain the months with more than 15 sky is clear days(day->month)
        JavaPairRDD<String, Integer> monthWeather = dayWeather.reduceByKey((x, y) -> x + y)
                .mapToPair(m -> new Tuple2<>(m._1.substring(0, m._1.length() - 3), m._2 >= 15 ? 1 : 0))
                .filter(m -> m._2 == 1);

        //Last reduce round to obtain the years with the 3 months of sky is clear (month -> year)
        JavaPairRDD<String, Integer> yearWeather = monthWeather.reduceByKey((x, y) -> x + y)
                .mapToPair(y -> new Tuple2<>(y._1, y._2 == 3 ? 1 : 0))
                .filter(y -> y._2 == 1);

        System.out.println(yearWeather.collect().toString());

        List<Tuple2<Integer, String>> query1Cities = yearWeather.mapToPair(y -> new Tuple2<>(Integer.parseInt(y._1.substring(y._1.length() - 4, y._1.length())), y._1.substring(0, y._1.length() - 4)))
                .sortByKey()
                .collect();

        System.out.println("Lista città con almeno 15 giorni di cielo sereno nei mesi di Marzo, Aprile, Maggio:  ");
        for (Tuple2<Integer, String> q : query1Cities) {
            System.out.println(q._1 + "  " + q._2);
        }

        Long endTime = System.nanoTime();

        Long timeElapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println("TIME ELAPSED -->  " + timeElapsed);

        //JavaPairRDD<LocalDateTime, Tuple2<String,Integer>> dayWeather = hourWeather.reduceByKey((x, y) -> x._2 + y._2 );

        sc.stop();

    }
}
