package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1 {
    public static void main(String[] args) {

        //Spark Configuration
        SparkConf conf = new SparkConf().setAppName("spark-tp-ventes").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // transformer le fichier ventes.txt en RDD
        JavaRDD<String> ventesLines = sc.textFile("ventes.txt");

        // transofrmation  d'un Autre RDD de type PaireRdd
        JavaPairRDD<String,Integer> ventesParVille = ventesLines.mapToPair(line -> {
            String [] lineDivise = line.split(" ");
            String ville = lineDivise[1];
            return new Tuple2<>(ville,1);
        }).reduceByKey(Integer::sum);

        // action forEach
        ventesParVille.collect().forEach(element -> System.out.println(element._1() + " : " + element._2()));



    }
}