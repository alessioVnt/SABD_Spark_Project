import Entity.CityTemperatureMisurements;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utilities.ForecastParser;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Query3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read from the csv
        JavaRDD<String> rawData = sc.textFile("data/temperature.csv");

        Long startTime = System.nanoTime();

        //Getting header line and parse for city name
        JavaRDD<String> headerRDD = rawData.filter(line -> line.contains("datetime"));
        String header = headerRDD.collect().get(0);
        ForecastParser.parseForecast(header);

        //Get data about 2016/2017 from raw file, excluding irrelevant months
        JavaRDD<CityTemperatureMisurements> cityTemperatureMisurementsJavaRDD = rawData.subtract(headerRDD).flatMap(line -> Objects.requireNonNull(ForecastParser.parseForecastTemperature(line)).iterator())
                .filter(y -> y.getYear() == 2016 || y.getYear() == 2017)
                .filter(w -> w.getMonth() != 5 && w.getMonth() != 10 && w.getMonth() != 11 && w.getMonth() != 12)
                .filter(h -> h.getHour() >= 12 && h.getHour() <= 15)
                //Catch error on temperature's data, assume t < 1000K
                .filter(t -> t.getTemperature() < 1000);

        //Create pairRDD filtered on years and months
        JavaPairRDD<String, Double> cityTemperatureWinter2016 = createFilteredPairRDD(cityTemperatureMisurementsJavaRDD, 2016, 1,4);
        JavaPairRDD<String, Double> cityTemperatureSummer2016 = createFilteredPairRDD(cityTemperatureMisurementsJavaRDD, 2016, 6,9);
        JavaPairRDD<String, Double> cityTemperatureWinter2017 = createFilteredPairRDD(cityTemperatureMisurementsJavaRDD, 2017, 1,4);
        JavaPairRDD<String, Double> cityTemperatureSummer2017 = createFilteredPairRDD(cityTemperatureMisurementsJavaRDD, 2017, 6,9);

        //Calculate the gap between the average temperatures of the same year in winter and summer months
        JavaPairRDD<String, Double> cityGap2016 = calculateAvg(cityTemperatureSummer2016)
                .union(calculateAvg(cityTemperatureWinter2016))
                .reduceByKey((x, y) -> x - y);
        JavaPairRDD<String, Double> cityGap2017 = calculateAvg(cityTemperatureSummer2017)
                .union(calculateAvg(cityTemperatureWinter2017))
                .reduceByKey((x, y) -> x - y);

        System.out.println("city gap 2016: " + cityGap2016.collect().toString());
        System.out.println("city gap 2017: " + cityGap2017.collect().toString());

        Long endTime = System.nanoTime();
        Long timeElapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("TIME ELAPSED -->  " + timeElapsed);
        sc.stop();
    }

    //Calculate average on values on a JavaPairRDD
    private static JavaPairRDD<String, Double> calculateAvg(JavaPairRDD<String, Double> javaPairRDD){
        return javaPairRDD
                .mapValues(value -> new Tuple2<>(value, 1))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapValues(value -> value._1 / value._2);
    }

    //Create a filtered pairRDD
    private static JavaPairRDD<String, Double> createFilteredPairRDD(JavaRDD<CityTemperatureMisurements> javaRDD, int year, int startMonth, int endMonth){
        return javaRDD
                .filter(y -> y.getYear() == year && y.getMonth() >= startMonth && y.getMonth() <= endMonth)
                .mapToPair(w -> new Tuple2<>(w.getCity(), w.getTemperature()));
    }
}
