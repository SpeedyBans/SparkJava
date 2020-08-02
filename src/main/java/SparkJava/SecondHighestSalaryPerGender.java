package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class SecondHighestSalaryPerGender {
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
        StructType SalarySchemma = new StructType()
                .add("Name","String",false)
                .add("Salary","Float",false)
                .add("Gender","String",false);

        /**----------------------------------------------------------------------------------------------------------------------
         * Reading local file as a DataSet
         * header: true -- mark true if the file contains header else mark false
         * delimiter:, -- used to pass a custom delimiter (not required if the delimiter is comma[,])
         * csv:C:/Users/Speedy/Downloads/503922_932309_bundle_archive/avengers.csv --we read the file usually as a csv,
         *     but can override the delimiter using option("delimiter","<custom delimiter>") if we have a different delimiter
         *-----------------------------------------------------------------------------------------------------------------------
         *  */
        Dataset<Row> DS = Spark.read().option("header",false).option("delimiter","|").schema(SalarySchemma).csv("hdfs://localhost:9000/data/input/SalaryDepartment.txt");
        //Dataset<Row> DSNewCol = Spark.read().option("header",true).option("delimiter",",").option("inferSchema",true).csv("C:/Users/Speedy/Downloads/503922_932309_bundle_archive/avengers.csv");
        DS.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        /**----------------------------------------------------------------------------------------------------------------------
         * We are using a window specification that defines the partitioning, ordering, and frame boundaries.
         * partitionBy: this is used to partition the data based on a particular column
         * orderBy: this is used to sort the data based on a particular column 
         *          we cannot order the column in descending order by default, and we are using functions function
         * functions: this is used to perform a sorting on data in descending order
         * col: this is used to instruct the functions on which column the action needs to be performed
         * desc: this is to perform the sorting in descending order
         * ----------------------------------------------------------------------------------------------------------------------
         */
        WindowSpec ws =Window.partitionBy("Gender").orderBy(functions.col("Salary").desc());

        /**----------------------------------------------------------------------------------------------------------------------
         * We are creating a new dataset with an additional column named Rank with row number per Gender as the value
         * withColumn: this is used to add a column to the existing Dataset DSL, it has two parameters
         * colName: this is the column name of the new field created
         * function: this is to perform a function over an attribute of
         * row_number: this is to assign row number, we can use rank as well in place of row_number
         * over: this to instruct row_number function on what column they have to add the row number
         * ----------------------------------------------------------------------------------------------------------------------
         */
        Dataset<Row> DSRank = DS.withColumn("Rank", functions.row_number().over(ws));
        //Dataset<Row> DSRank = DS.withColumn("Rank", functions.rank().over(ws));
        DSRank.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> secondDS = DSRank.filter(DSRank.col("Rank").equalTo(2));//We are filtering only those records which have the Rank equals to 2
        // so that get second highest per partition
        secondDS.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> finalDS = secondDS.drop("Rank");//we are dropping the intermediate column "Rank" added to calculate the rank per partition
        finalDS.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Spark.close();//to close the session at the end of the program (it is always suggested to keep the logic in final block,
        //              but since the we are creating basic program and will not run into exception we are good)
    }
}
