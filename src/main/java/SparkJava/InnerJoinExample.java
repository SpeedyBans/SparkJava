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
import java.util.List;

public class InnerJoinExample {
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
		
		List<Tuple2<Integer,Integer>> visits = new ArrayList<>();
		List<Tuple2<Integer,String>> users = new ArrayList<>();
		
		visits.add(new Tuple2<>(4, 18));
		visits.add(new Tuple2<>(6, 4));
		visits.add(new Tuple2<>(10, 9));
		
		users.add(new Tuple2<>(1,"John"));
		users.add(new Tuple2<>(2,"Bob"));
		users.add(new Tuple2<>(3,"Alan"));
		users.add(new Tuple2<>(4,"Doris"));
		users.add(new Tuple2<>(5,"Marybelle"));
		users.add(new Tuple2<>(6,"Raquel"));
		
		JavaPairRDD<Integer, Integer> visitsRDD = sc.parallelizePairs(visits);
		JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);
		
		JavaPairRDD joinedRDD = visitsRDD.join(usersRDD);//The RDD structure will be <Integer, Tuple2<Integer,String>>
		// but it can automatically resolve the type as well
		joinedRDD.collect().forEach(System.out::println);
		
		sc.close();//to close the spark context at the end of the program
		Spark.close();//to close the spark session at the end of the program
		//We should keep the close function inside a final block but since we are not working on a complex code, its ok to add at the end of the program too
	}
}
