import Entity.CityTemperatureMisurements;
import Entity.Query3Out;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utilities.ForecastParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Query3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //Read from the csv
        JavaRDD<String> rawData = sc.textFile("data/temperature.csv");
        //Getting the cities/nations hashmap
        ForecastParser.parseLatLong(sc.textFile("data/city_attributes.csv").filter(line -> !line.contains("City,Latitude")).collect());

        Long startTime = System.nanoTime();

        //Getting header line and parse for city name
        JavaRDD<String> headerRDD = rawData.filter(line -> line.contains("datetime"));
        String header = headerRDD.collect().get(0);
        ForecastParser.parseForecast(header);


        //Get data about 2016/2017 from raw file, excluding irrelevant months
        JavaRDD<CityTemperatureMisurements> cityTemperatureMisurementsJavaRDD = rawData.subtract(headerRDD).flatMap(line -> Objects.requireNonNull(ForecastParser.parseForecastTemperature(line)).iterator())
                .filter(y -> y.getYear() == 2016 || y.getYear() == 2017)
                .map(ctm -> ForecastParser.convertMeasurement(ctm))
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

        //Creation of an RDD with nations as key
        JavaPairRDD<String, Tuple2<String, Double>> nationsGap2016 = cityGap2016.mapToPair(c -> new Tuple2<>(ForecastParser.getNation(c._1), new Tuple2<>(c._1, c._2)));
        JavaPairRDD<String, Tuple2<String, Double>> nationsGap2017 = cityGap2017.mapToPair(c -> new Tuple2<>(ForecastParser.getNation(c._1), new Tuple2<>(c._1, c._2)));

        //Collect the list of nations that are contained in RDDs
        List<String> nationKeys = nationsGap2016.keys().distinct().collect();

        //This list will contain all the RDD (1 for each nation), that contain the pair nation/city - temperature gap
        List<JavaPairRDD<String,Double>> cityGaps2016List = new ArrayList<>();
        List<JavaPairRDD<String,Double>> cityGaps2017List = new ArrayList<>();

        //list that will contain the RDD with the outputs
        List<JavaRDD<Query3Out>> outputList = new ArrayList<>();

        for (String nation: nationKeys) {

            JavaPairRDD<String, Double> cityGapsByNation16 = nationsGap2016.filter(n -> n._1.equals(nation)).mapToPair(n -> new Tuple2<>(n._1 + "/" + n._2._1, n._2._2));
            cityGaps2016List.add(cityGapsByNation16);

            JavaPairRDD<Double, String> invertedCityGaps16 = cityGapsByNation16.mapToPair(c -> new Tuple2<>(c._2, c._1)).sortByKey();
            List<Tuple2<Double, String>> top316 = invertedCityGaps16.take(3);
            //Creation of RDD<Query3Out> that contains the POJO for the dataframe
            outputList.add(sc.parallelize(top316).map(x -> new Query3Out(x._2, "2016", x._1)));


            JavaPairRDD<String, Double> cityGapsByNation17 = nationsGap2017.filter(n -> n._1.equals(nation)).mapToPair(n -> new Tuple2<>(n._1 + "/" + n._2._1, n._2._2));
            cityGaps2017List.add(cityGapsByNation17);

            JavaPairRDD<Double, String> invertedCityGaps17 = cityGapsByNation17.mapToPair(c -> new Tuple2<>(c._2, c._1)).sortByKey();
            List<Tuple2<Double, String>> top317 = invertedCityGaps17.take(3);

            //Creation of RDD<Query3Out> that contains the POJO for the dataframe
            outputList.add(sc.parallelize(top317).map(x -> new Query3Out(x._2, "2017", x._1)));

        }

        //Unifying the outputs in one RDD
        JavaRDD<Query3Out> output = outputList.get(0);

        boolean first = true;

        for (JavaRDD<Query3Out> out: outputList) {
            if (!first) output = output.union(out);
            else first = false;
        }

        //Creation of dataframe that will be converted in Json
        SparkSession spark = SparkSession.builder()
                .appName("Query3DF")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        //Dataframe creation from RDD
        Dataset<Row> outDF =  spark.createDataFrame(output, Query3Out.class);

        //convert dataframe to Json and save in HDFS
        outDF.coalesce(1).write().format("json").save("hdfs://localhost:54310/output/query3/q3output");

        //Stopping timer for execution time metrics
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
