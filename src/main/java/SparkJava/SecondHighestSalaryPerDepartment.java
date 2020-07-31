package SparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class SecondHighestSalaryPerDepartment {
    public static void main (String[] args) throws Exception
    {
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
        StructType employeeSchema = new StructType()
                .add("EmployeeID","Long")
                .add("EmployeeName","String")
                .add("EmployeeSalary","Double")
                .add("DepartmentID","Long");

        StructType departmentSchema = new StructType()
                .add("DepartmentID","Long")
                .add("DepartmentName","String");

        /**----------------------------------------------------------------------------------------------------------------------
         * Reading local file as a DataSet
         * header: true -- mark true if the file contains header else mark false
         * delimiter:, -- used to pass a custom delimiter (not required if the delimiter is comma[,])
         * csv:C:/Users/Speedy/Downloads/503922_932309_bundle_archive/avengers.csv --we read the file usually as a csv,
         *     but can override the delimiter using option("delimiter","<custom delimiter>") if we have a different delimiter
         *-----------------------------------------------------------------------------------------------------------------------
         *  */
        Dataset<Row> employee = Spark.read()
                .option("header",true)
                .option("delimiter","|")
                .schema(employeeSchema)
                .csv("hdfs://localhost:9000/data/input/emp.txt");
        System.out.println("Printing the Schema of Dataset: employee");
        employee.printSchema();//To show the schema of the dataset
        System.out.println("Printing the data for Dataset: employee");
        employee.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> department = Spark.read()
                .option("header",true)
                .option("delimiter","|")
                .schema(departmentSchema)
                .csv("hdfs://localhost:9000/data/input/dept.txt");
        System.out.println("Printing the Schema of Dataset: department");
        department.printSchema();
        System.out.println("Printing the data for Dataset: department");
        department.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> Employee = employee.withColumnRenamed("DepartmentID","DepartmentIDEmp");
        System.out.println("Printing the Schema of Dataset: Employee");
        Employee.printSchema();
        System.out.println("Printing the data for Dataset: Employee");
        Employee.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> empDept = Employee//.toDF("DepartmentID","DepartmentIDEmp")
                //.withColumnRenamed("DepartmentID","DepartmentIDEmp")  //to rename column DepartmentID to DepartmentIDEmp
                .join(department, Employee
                        .col("DepartmentIDEmp"/*"DepartmentID"/*"DepartmentIDEmp*/)//.alias("DepartmentIDEmp") //since rename function is not working, I have changed the join condition column to DepartmentID
                        .equalTo(department.col("DepartmentID")))//;
                .select("EmployeeID","EmployeeName","EmployeeSalary","EmployeeSalary","DepartmentID","DepartmentName");//to select only mentioned column and drop the remaining columns
                //commented the above code as the code keeps failing for unambiguous field name DepartmentID <UPDATE:renamed the column to DepartmentIDEmp in line number 90>
        System.out.println("Printing the Schema of Dataset: empDept");
        empDept.printSchema();
        System.out.println("Printing the data of Dataset: empDept");
        empDept.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

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
        WindowSpec ws = Window.partitionBy("DepartmentName").orderBy(functions.col("EmployeeSalary").desc());

        /**----------------------------------------------------------------------------------------------------------------------
         * We are creating a new dataset with an additional column named Rank with row number per Gender as the value
         * withColumn: this is used to add a column to the existing Dataset DSL, it has two parameters
         * colName: this is the column name of the new field created
         * function: this is to perform a function over an attribute of
         * row_number: this is to assign row number, we can use rank as well in place of row_number
         * over: this to instruct row_number function on what column they have to add the row number
         * ----------------------------------------------------------------------------------------------------------------------
         */
        Dataset<Row> DSRank = empDept.withColumn("Rank", functions.row_number().over(ws));
        //Dataset<Row> DSRank = DS.withColumn("Rank", functions.rank().over(ws));
        System.out.println("Printing the Schema of Dataset: DSRank");
        DSRank.printSchema();
        System.out.println("Printing the data of Dataset: DSRank");
        DSRank.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> secondRank = DSRank.filter(DSRank.col("Rank").equalTo(2));//We are filtering only those records which have the Rank equals to 2
        // so that get second highest per partition
        System.out.println("Printing the Schema of Dataset: secondRank");
        secondRank.printSchema();
        System.out.println("Printing the data of Dataset: secondRank");
        secondRank.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        Dataset<Row> finalDS = secondRank.drop("Rank");//we are dropping the intermediate column "Rank" added to calculate the rank per partition
        System.out.println("Printing the Schema of Dataset: finalDS");
        finalDS.printSchema();
        System.out.println("Printing the data of Dataset: finalDS");
        finalDS.show();//to show the Dataset, we do not have to run a loop on DataSet unlike RDD as this DataSet is stored more like a table and not like file

        sc.close();//to close the spark context at the end of the program
        Spark.close();//to close the spark session at the end of the program
        //We should keep the close function inside a final block but since we are not working on a complex code, its ok to add at the end of the program too
    }
}
