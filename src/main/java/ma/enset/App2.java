package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        // de calculer le prix total
        //des ventes des produits par ville
        //Spark Configuration
        SparkConf conf = new SparkConf().setAppName("spark-tp2").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // transformer le fichier ventes.txt en RDD
        JavaRDD<String> ventesLines = sc.textFile("ventes.txt");

        JavaPairRDD<String,Double> prixTotalParVille = ventesLines.mapToPair(item ->

                  new Tuple2<String,Double>(item.split(" ")[1],Double.parseDouble(item.split(" ")[3]))
                ).reduceByKey(Double::sum);
        prixTotalParVille.collect().forEach(element -> System.out.println(element._1 + " " + element._2));
    }
}
