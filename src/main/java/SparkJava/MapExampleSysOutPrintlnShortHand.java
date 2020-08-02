package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class MapExampleSysOutPrintlnShortHand {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);// to filter out only errors in the console

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

        /**----------------------------------------------------------------------------------------------------------------------
         * Creating an Spark Session
         * Instead of Spark Conf and Context we can directly create a Spark session
         * appName:New Spark Ingestion -- this is a name of the Spark session just like Spark Conf
         * master:local[*] -- this is to tell that the session that we are running is in local mode
         *-----------------------------------------------------------------------------------------------------------------------
         * */
        SparkSession Spark = SparkSession.builder().appName("New Spark Ingestion").master("local[*]").getOrCreate();

        /**----------------------------------------------------------------------------------------------------------------------
         * We are creating a Schema by using a StructType (it extends the class DataType)
         * we can define the column name, column datatype and whether or not that field can be nullable or not
         * Name: the first parameter is the name of the column Eg: "Name"
         * dataType: the second parameter is the datatype of the column Eg: "Sting"
         * nullable: the third parameter is the Nullable or not nullable
         *           (If nullable we have to pass true, for notnull we have to pass false), this is optional,
         *           by default it takes it as a nullable field
         * comment: the 4th parameter is comment of the attribute
         * ----------------------------------------------------------------------------------------------------------------------
         */

        List<Integer> inputData = new ArrayList<>();//to create a list of values in order to pass to Order
        inputData.add(16);
        inputData.add(748);
        inputData.add(2084);
        inputData.add(9362);
        inputData.add(682);
        inputData.add(234);
        inputData.add(1);
        inputData.add(632);
        inputData.add(912);
        inputData.add(323);
        inputData.add(2890);
        inputData.add(438);
        inputData.add(689);
        inputData.add(435);
        inputData.add(4);
        inputData.add(4328);

        JavaRDD<Integer> myRDD = sc.parallelize(inputData);

        JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));//we are using Java math function to calculate the square root of the RDD

        System.out.println(":::::::::::::::::::::::OUTPUT:::::::::::::::::::::::::::");
        System.out.println("Printing the sqrtRDD: ");
        sqrtRDD.collect().forEach(System.out::println);//In complex CPU if we use sqrtRDD.foreach(System.out::println),
        // it crashes with the error not able to serialize.
        //to fix that we add collect() function and with collect function we can't use foreach(System.out::println)
        // instead we will have to use forEach(System.out::println)

        sc.close();//to close the spark context at the end of the program
        Spark.close();//to close the spark session at the end of the program
        //We should keep the close function inside a final block but since we are not working on a complex code, its ok to add at the end of the program too
    }
}
