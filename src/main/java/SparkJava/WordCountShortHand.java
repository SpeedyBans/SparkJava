package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountShortHand {

    public static void main(String [] ar)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);// to filter out only Warning and errors in the console

        /**----------------------------------------------------------------------------------------------------------------------------
         * Creating a Spark configuration as unlike Scala we do not have a default spark context created as the start of the session
         * setAppName:MySparkProject this is to give a name to the spark conf
         * setMaster:local[*] this is to run the spark conf in local mode
         * Once we create a spark conf we will have to create a Java Spark Context , We will be leveraging the Spark thru SparkContext
         *
         * Question: how will the functionality vary if we use Spark Context rather than a Java Spark Context
         * ----------------------------------------------------------------------------------------------------------------------------
         * */
        SparkConf conf = new SparkConf().setAppName("MySparkProject").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> RDD = sc.textFile("hdfs://localhost:9000/README.md");//reading the text file as RDD

        JavaPairRDD<String, Integer> wordCount = RDD
                .flatMap(s -> Arrays.asList(s.split(" "))
                .iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        wordCount.foreach(line -> System.out.println(line));//to print each line of the RDD we have to run it in a loop by reading each line and printing

        wordCount.saveAsTextFile("hdfs://localhost:9000/data/output/WordCount");//To save the RDD in an output file
    }
}

