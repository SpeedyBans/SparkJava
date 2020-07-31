package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ReadHdfsData {
    public static void  main(String [] ag)
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

        /**---------------------------------------------------------------------------------------------------------------------------
         * below we are reading HDFS data as RDD
         * [hdfs://]: Mentioned this so that Spark understands that it has to read a HDFS file
         * [localhost:9000]: this is mentioned so that Spark knows which host to connect to
         *                   in our case since the Hadoop is running in our local we gave local host
         *                   since we are running Hadoop in windows machine we gave the port as 9000
         *                   {not sure though whether this will change for LINUX and Mac machines}
         * ---------------------------------------------------------------------------------------------------------------------------
         */
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/README.md");//reading the HDFS data as RDD

        lines.foreach(new VoidFunction<String>()//reading each line with the help of VoidFunction
          {
              public void call (String line )
              {
                  System.out.println("* "+line);
              }
          }
        );
    }
}
