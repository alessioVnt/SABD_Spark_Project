
/*Individuare, per ogni nazione, la media, la deviazione standard, il minimo, il massimo della temperatura, della pressione e dell’umidita
registrata in ogni mese di ogni anno. `
Nota: la nazione a cui appartiene ogni citta non viene indicata in modo esplicito nel dataset, ma deve `
essere ricavata.
*/

import Entity.Query2Out;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utilities.MeasurementParser;
import utilities.MeasurementStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Query2 {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#2");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Long startTime = System.nanoTime();

        JavaRDD<String> humidityRaw = sc.textFile("data/humidity.csv");
        JavaRDD<String> pressureRaw = sc.textFile("data/pressure.csv");
        JavaRDD<String> temperatureRaw = sc.textFile("data/temperature.csv");

        JavaRDD<String> latLongRaw = sc.textFile("data/city_attributes.csv")
                .filter(line -> !line.contains("City,Lati"));

        Long startMapBoxTime = System.nanoTime();

        //Obtaining the country corresponding to each city that will be saved into the MeasurementParser HashMap cityNationMap attribute
        MeasurementParser.parseLatLong(latLongRaw.collect());

        Long endMapBoxTime = System.nanoTime();

        //Parsing the header of 1 of the CSV to obtain the cities list
        MeasurementParser.parseHeader(humidityRaw.filter(line -> line.contains("datetime")).collect().get(0));

        //Humidity: Creation <key, value> couple.
        //@Param: key: composed by nation + year + month
        //@Param: value: humidity value
        JavaPairRDD<String, Double> humidityRdd = humidityRaw.filter(line -> !line.contains("datetime"))
                .flatMapToPair(line -> MeasurementParser.parseMeasurement(line, "HUM").iterator());

        //Compute Mean, Std Dev, Max and Min trough statisticsCompute method of MeasurementStatistics class
        JavaPairRDD<String, ArrayList<Double>> humidityStat = humidityRdd.groupByKey()
                .mapValues(MeasurementStatistics::statisticsCompute)
                .sortByKey();



        //Pressure: Creation <key, value> couple.
        //@Param: key: composed by nation + year + month
        //@Param: value: pressure value
        JavaPairRDD<String, Double> pressureRdd = pressureRaw.filter(line -> !line.contains("datetime"))
                .flatMapToPair(line -> MeasurementParser.parseMeasurement(line, "PRES").iterator());

        //Compute Mean, Std Dev, Max and Min trough statisticsCompute method of MeasurementStatistics class
        JavaPairRDD<String, ArrayList<Double>> pressureStat = pressureRdd.groupByKey()
                .mapValues(MeasurementStatistics::statisticsCompute)
                .sortByKey();

        //Temperature: Creation <key, value> couple.
        //@Param: key: composed by nation + year + month
        //@Param: value: temperature value
        //Error check made by checking that temperature doesn't go over 350K
        // In case of error the entry is not created (error elimination above error correction)
        JavaPairRDD<String, Double> temperatureRdd = temperatureRaw.filter(line -> !line.contains("datetime"))
                .flatMapToPair(line -> MeasurementParser.parseMeasurement(line, "TEMP").iterator());

        //Compute Mean, Std Dev, Max and Min trough statisticsCompute method of MeasurementStatistics class
        JavaPairRDD<String, ArrayList<Double>> temperatureStat = temperatureRdd.groupByKey()
                .mapValues(MeasurementStatistics::statisticsCompute)
                .sortByKey();

        Long endTime = System.nanoTime();

        //Execution time computation
        long timeElapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        long mapBoxTimeElapsed = TimeUnit.NANOSECONDS.toMillis((endMapBoxTime - startMapBoxTime));

        //Execution time
        System.out.println("MAPBOX TIME ELAPSED --> " + mapBoxTimeElapsed);
        System.out.println("TIME ELAPSED -->  " + timeElapsed);


        //Results print for debug purpose only
        //TEMPERATURE
        /*
        List<Tuple2<String, ArrayList<Double>>> tempResults = temperatureStat.take(20);
        for (Tuple2<String, ArrayList<Double>> result: tempResults) {
            System.out.println("Country: " + result._1.substring(0, result._1.length() - 7) + " Year: " + result._1.substring(result._1.length() - 7, result._1.length() - 3) + " Month: " + result._1.substring(result._1.length() - 2, result._1.length()));
            System.out.println("Temperature: mean: " + result._2.get(0) + " Std dev: " + result._2.get(1) + " max: " + result._2.get(2) + " min: " + result._2.get(3));
        }
        //HUMIDITY
        List<Tuple2<String, ArrayList<Double>>> humResults = humidityStat.take(20);
        for (Tuple2<String, ArrayList<Double>> result: humResults) {
            System.out.println("Country: " + result._1.substring(0, result._1.length() - 7) + " Year: " + result._1.substring(result._1.length() - 7, result._1.length() - 3) + " Month: " + result._1.substring(result._1.length() - 2, result._1.length()));
            System.out.println("Humidity: mean: " + result._2.get(0) + " Std dev: " + result._2.get(1) + " max: " + result._2.get(2) + " min: " + result._2.get(3));
        }
        //PRESSURE
        List<Tuple2<String, ArrayList<Double>>> presResults = pressureStat.take(20);
        for (Tuple2<String, ArrayList<Double>> result: presResults) {
            System.out.println("Country: " + result._1.substring(0, result._1.length() - 7) + " Year: " + result._1.substring(result._1.length() - 7, result._1.length() - 3) + " Month: " + result._1.substring(result._1.length() - 2, result._1.length()));
            System.out.println("Pressure: mean: " + result._2.get(0) + " Std dev: " + result._2.get(1) + " max: " + result._2.get(2) + " min: " + result._2.get(3));
        }
        */


        //Get POJO from results RDD
        //POJO RDD for temperature
        JavaRDD<Query2Out> tempQuery2Out = temperatureStat.map(ts -> new Query2Out(ts._1.substring(0, ts._1.length() - 7), ts._1.substring(ts._1.length() - 7, ts._1.length() - 3), ts._1.substring(ts._1.length() - 2, ts._1.length()), "Temperature",  ts._2.get(0), ts._2.get(1), ts._2.get(2), ts._2.get(3)));

        //POJO RDD for humidity
        JavaRDD<Query2Out> humQuery2Out = humidityStat.map(ts -> new Query2Out(ts._1.substring(0, ts._1.length() - 7), ts._1.substring(ts._1.length() - 7, ts._1.length() - 3), ts._1.substring(ts._1.length() - 2, ts._1.length()), "Humidity",  ts._2.get(0), ts._2.get(1), ts._2.get(2), ts._2.get(3)));

        //POJO RDD for pressure
        JavaRDD<Query2Out> pressQuery2Out = pressureStat.map(ts -> new Query2Out(ts._1.substring(0, ts._1.length() - 7), ts._1.substring(ts._1.length() - 7, ts._1.length() - 3), ts._1.substring(ts._1.length() - 2, ts._1.length()), "Pressure",  ts._2.get(0), ts._2.get(1), ts._2.get(2), ts._2.get(3)));

        //union on the 3 RDD
        JavaRDD<Query2Out> output = tempQuery2Out.union(humQuery2Out)
                .union(pressQuery2Out);

        System.out.println(output.take(1).get(0).getCountry());

        //Creation of dataframe that will be converted in Json
        SparkSession spark = SparkSession.builder()
                .appName("Query2DF")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        //Dataframe creation from RDD
        Dataset<Row> outDF =  spark.createDataFrame(output, Query2Out.class);

        //convert dataframe to Json and save in HDFS
        outDF.coalesce(1).write().format("json").save("hdfs://localhost:54310/output/query2output");

        sc.stop();
    }
}
