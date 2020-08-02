package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class ReadFileWithSchema {
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
        StructType salarySchema1 = new StructType()
                .add("Name","String",false)
                .add("Salary","Float");

        StructType salarySchema2 = new StructType()
                .add("Name","String",false)
                .add("Salary","Float", false, "This is a sample comment for Salary field")
                .add("Age","Int",true);

        /**----------------------------------------------------------------------------------------------------------------------
         * Reading local file as a DataSet
         * header: true -- mark true if the file contains header else mark false
         * delimiter:, -- used to pass a custom delimiter (not required if the delimiter is comma[,])
         * csv:hdfs://localhost:9000/data/input/Salary1.txt --we are reading a HDFS file here and usually its always read as a csv,
         *     but can override the delimiter using option("delimiter","<custom delimiter>") if we have a different delimiter
         *-----------------------------------------------------------------------------------------------------------------------
         *  */
        Dataset<Row> Salary1 = Spark.read().option("header",false).option("delimiter","|").schema(salarySchema1).csv("hdfs://localhost:9000/data/input/Salary1.txt");
        Dataset<Row> Salary2 = Spark.read().option("header",false).option("delimiter","|").schema(salarySchema2).csv("C:\\Users\\Speedy\\Documents\\Salary2.txt");

        Salary1.printSchema();//to show the schema of the dataset. this will show what is the datatype of the attribute, whether its nullable or not Eg:|-- Name: string (nullable = true)
        Salary2.printSchema();//to show the schema of the dataset. this will show what is the datatype of the attribute, whether its nullable or not Eg:|-- Name: string (nullable = true)

        Salary1.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file
        Salary2.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Spark.close();//to close the session at the end of the program (it is always suggested to keep the logic in final block,
        //              but since the we are creating basic program and will not run into exception we are good)
    }
}