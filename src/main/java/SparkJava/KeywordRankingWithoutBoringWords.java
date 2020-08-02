package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class KeywordRankingWithoutBoringWords {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);// to filter out only Warning and errors in the console

        /**----------------------------------------------------------------------------------------------------------------------------
         * Creating a Spark configuration as unlike Scala we do not have a default spark context created as the start of the session
         * setAppName:MySparkProject this is to give a name to the spark conf
         * setMaster:local[*] this is to run the spark conf in local mode
         * Once we create a spark conf we will have to create a Java Spark Context , We will be leveraging the Spark thru SparkContext
         *
         * Question: how will the functionality vary if we use Spark Context rather than a Java Spark Context
         * ----------------------------------------------------------------------------------------------------------------------------
         * */
        SparkConf conf = new SparkConf().setAppName("MySparkProject").setMaster("local");
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

        JavaRDD<String> myRDD = sc.textFile("hdfs://localhost:9000/data/input/input.txt");

        JavaRDD<String> stringsOnly = myRDD.map(line -> line.replaceAll("[^A-Za-z\\s]","").toLowerCase());

        JavaRDD<String> withoutBlankLine = stringsOnly.filter(line -> line.trim().length() > 0);

        JavaRDD<String> flattened = withoutBlankLine.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaRDD<String> trimmed = flattened.filter(word -> word.trim().length() >0);

        JavaRDD<String> withoutBoring = trimmed.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String,Long> interestingWords = withoutBoring.mapToPair(word -> new Tuple2<>(word,1L));

        JavaPairRDD<String,Long> counts = interestingWords.reduceByKey((value1,value2) -> value1+value2);

        JavaPairRDD<Long,String> switched = counts.mapToPair(tuple -> new Tuple2<>(tuple._2,tuple._1));

        JavaPairRDD<Long,String> sorted = switched.sortByKey(false);

        List<Tuple2<Long,String>> result = sorted.take(10);
        result.forEach(System.out::println);

        sc.close();//to close the spark context at the end of the program
        Spark.close();//to close the spark session at the end of the program
        //We should keep the close function inside a final block but since we are not working on a complex code, its ok to add at the end of the program too
    }
}
