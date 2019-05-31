package utilities;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsReader {

    private static final String NAME_NODE = "hdfs://localhost:50070/file";//nameNomeHost = localhost if you use hadoop in local mode

    public static void main(String[] args) throws URISyntaxException, IOException {
        //String fileInHdfs = "/FILE";
        //FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());
        //String fileContent = IOUtils.toString(fs.open(new Path(fileInHdfs)), "UTF-8");
        //System.out.println("File content - " + fileContent);
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Forecast Query#1");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> prova = sc.textFile("hdfs://localhost:54310/output/query3output/part-00000-eb77068f-91f4-42d9-96d1-a52baee074e9-c000.json");
        System.out.println(prova.collect().toString());
    }

}