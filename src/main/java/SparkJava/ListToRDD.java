package SparkJava;

import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class ListToRDD {
    public static void main (String[] a)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);// to filter out only Warning and errors in the console

        List<Double> inputData = new ArrayList<>();//creating a list of double type

        //adding numbers to the list
        inputData.add(12.4);
        inputData.add((double) 12);
        inputData.add(23.46);
        inputData.add(876.4);
        inputData.add(928.367);
        inputData.add(6578.342);

        /**----------------------------------------------------------------------------------------------------------------------------
         * Creating a Spark configuration as unlike Scala we do not have a default spark context created as the start of the session
         * setAppName:MySparkProject this is to give a name to the spark conf
         * setMaster:local[*] this is to run the spark conf in local mode
         * Once we create a spark conf we will have to create a Java Spark Context , We will be leveraging the Spark thru SparkContext
         *
         * Question: how will the functionality vary if we use Spark Context rather than a Java Spark Context
         * ----------------------------------------------------------------------------------------------------------------------------
         * */
        SparkConf conf = new SparkConf().setAppName("Spark Java").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**----------------------------------------------------------------------------------------------------------------------------
         * Creating a Resilient Distributed Data with the name RDD
         * this is same as Multi file in Hadoop and Ab initio
         * the data will be distributed in multiple partition to that spark can work on each partition parallel
         * ---------------------------------------------------------------------------------------------------------------------------
         * */
        JavaRDD<Double> RDD = sc.parallelize(inputData);

        System.out.println(":::::::::::::Printing the RDD::::::::::::::");
        RDD.foreach(line -> System.out.println(line));//to print each line of the RDD we have to run it in a loop by reading each line and printing

        sc.close();//to close the session at the end of the program (it is always suggested to keep the logic in final block,
        //              but since the we are creating basic program and will not run into exception we are good)
    }
}
