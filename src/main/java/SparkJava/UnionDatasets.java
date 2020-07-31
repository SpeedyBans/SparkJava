package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnionDatasets {
    public static void main(String[] s)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);//to remove un necessary logs and show only errors and warnings

        /**----------------------------------------------------------------------------------------------------------------------
         * Creating an Spark Session
         * Instead of Spark Conf and Context we can directly create a Spark session
         * appName:New Spark Ingestion -- this is a name of the Spark session just like Spark Conf
         * master:local[*] -- this is to tell that the session that we are running is in local mode
         *-----------------------------------------------------------------------------------------------------------------------
         * */
        SparkSession Spark = SparkSession.builder().appName("New Spark Ingestion").master("local[*]").getOrCreate();

        /**----------------------------------------------------------------------------------------------------------------------
         * Reading local file as a DataSet
         * header: true -- mark true if the file contains header else mark false
         * delimiter:, -- used to pass a custom delimiter (not required if the delimiter is comma[,])
         * csv:C:/Users/Speedy/Downloads/503922_932309_bundle_archive/avengers.csv --we read the file usually as a csv,
         *     but can override the delimiter using option("delimiter","<custom delimiter>") if we have a different delimiter
         *-----------------------------------------------------------------------------------------------------------------------
         *  */
        Dataset<Row> Salary1 = Spark.read().option("header",false).option("delimiter","|").option("inferSchema",true).csv("hdfs://localhost:9000/data/input/Salary1.txt");
        Dataset<Row> Salary2 = Spark.read().option("header",false).option("delimiter","|").option("inferSchema",true).csv("hdfs://localhost:9000/data/input/Salary2.txt");

        Salary1.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file
        Salary2.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> SalaryCombined = Salary1.union(Salary2);

        SalaryCombined.show();

        Spark.close();//to close the session at the end of the program (it is always suggested to keep the logic in final block,
        //              but since the we are creating basic program and will not run into exception we are good)

    }
}