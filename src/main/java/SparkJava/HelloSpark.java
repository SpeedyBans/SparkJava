package SparkJava;

import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class HelloSpark {
    public static void main (String [] arg)
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

        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
        list.add (new Tuple2 ("Java",29));
        list.add (new Tuple2 ("Hadoop",30));
        list.add (new Tuple2 ("Scala",31));
        list.add (new Tuple2 ("Spark",32));

        /**----------------------------------------------------------------------------------------------------------------------------
         * Creating a paired Resilient Distributed Data with the name RDD
         * this is same as a normal RDD but will have a key value pair
         * the data will be distributed in multiple partition to that spark can work on each partition parallel
         * ---------------------------------------------------------------------------------------------------------------------------
         * */
        JavaPairRDD<String, Integer> RDD = sc.parallelizePairs(list);
        RDD.foreach(line -> System.out.println(line));//to print each line of the RDD we have to run it in a loop by reading each line and printing

        sc.close();//to close the session at the end of the program (it is always suggested to keep the logic in final block,
        //              but since the we are creating basic program and will not run into exception we are good)
    }
}
