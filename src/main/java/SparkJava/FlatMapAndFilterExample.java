package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapAndFilterExample {
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

        List<String> inputData = new ArrayList<>();//to create a list of values in order to pass to Order
        inputData.add("ERROR: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 4 September 0406");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Tuesday 4 September 1632");
        inputData.add("ERROR: Tuesday 4 September 1854");
        inputData.add("WARN: Tuesday 4 September 1942");

        JavaRDD<String> myRDD = sc.parallelize(inputData);

        //Since there is no method in String that returns a collection of strings, we will have to use Array
        //Array has a function called asList, we can use that to create an Array list, but it still keeps throwing a compile error
        //The API specifically demands that we return an object of type iterator, that is why we are using .iterator() function
        JavaRDD<String> words = myRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

        JavaRDD<String> filteredRDD = words.filter(word -> word.length() > 1);
        System.out.println(":::::::::::::Output::::::::::::::");
        filteredRDD.collect().forEach(System.out::println);

        sc.close();//to close the spark context at the end of the program
        Spark.close();//to close the spark session at the end of the program
        //We should keep the close function inside a final block but since we are not working on a complex code, its ok to add at the end of the program too
    }
}
