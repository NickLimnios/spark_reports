import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object sparkreportsapp {

    //declare enviromental variables
    val datasetRootPath : String = "/home/lab/repos/spark_reports/dataset/"
    val outputRootPath : String = "/home/lab/repos/spark_reports/export/"

    def main(args:Array[String])= { 

        //Console.println("Starting program with arg[0]=" + args(0))

        //setup spark session
        Console.println("starting spark....")
        val spark : SparkSession = SparkSession.builder().master("local[*]").appName("Project2").getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("OFF")
        
        
        //load dataframes
        Console.println("loading dataframes....")
        val aislesDF = loadDataFrame(aisles(), datasetRootPath.concat("aisles.csv"))
        val departmentsDF = loadDataFrame(departments(), datasetRootPath.concat("departments.csv"))
        val productsDF = loadDataFrame(products(), datasetRootPath.concat("products.csv"))
        val ordersDF = loadDataFrame(orders(), datasetRootPath.concat("orders.csv"))
        val orderProductsDF = loadDataFrame(orderProducts(), datasetRootPath.concat("order_products.csv"))

        
        //app logic
        // if (args(0) == "1")
        // {
             getTotalProductsPerDepartmentReport(departmentsDF, productsDF, outputRootPath)
        // }
        // else if (args(0) == "2")
        // {
             getTotalOrdersPerWeekdayReport(ordersDF, outputRootPath)
        // }
        // else if (args(0) == "3")
        // {
             getProductsPerDepartmentOrderedOnceFromCustomers(departmentsDF, productsDF, orderProductsDF, outputRootPath)
        // }
        // else if (args(0) == "4")
        // {
             getMostOrderedProductsPerDepartment(departmentsDF, productsDF, orderProductsDF, outputRootPath)
        // }


        //stop spark
        Console.println("stopping spark....")

        spark.stop()
    }

    def loadDataFrame(schema : StructType, path : String):DataFrame = {
        val spark = SparkSession.builder.getOrCreate()
        val df = spark.read.schema(schema).csv(path)
        return df
    }

    // get report methods
    def getTotalProductsPerDepartmentReport(departmentsDF : DataFrame, productsDF : DataFrame, outputRootPath : String) =
    {
        Console.println("getting total products per department report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentProducts temp view....")
        val departmentProductsDF = departmentsDF.join(productsDF, "department_id")
        departmentProductsDF.createOrReplaceTempView("departmentProducts")

        val cmd = "select department as Department_Name, count(*) as Products_Count from departmentProducts group by department order by department"
        Console.println(cmd)
        val result = spark.sql(cmd)

        Console.println("showing totalProductsPerDepartment report....")
        result.show()

        Console.println("exporting totalProductsPerDepartment report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("totalProductsPerDepartment"))

    }

    def getTotalOrdersPerWeekdayReport(ordersDF : DataFrame, outputRootPath : String) = {
        Console.println("getting total orders per weekday report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create orders temp view....")
        ordersDF.createOrReplaceTempView("orders")

         val cmd = "select order_dow as Day_Of_Week, count(*) as Orders_Count from orders group by order_dow order by order_dow asc"
        Console.println(cmd)
        val result = spark.sql(cmd)

        Console.println("showing totalOrdersPerWeekdayReport report....")
        result.show()

        Console.println("exporting totalOrdersPerWeekdayReport report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("totalOrdersPerWeekdayReport"))

    }

    def getProductsPerDepartmentOrderedOnceFromCustomers(departmentsDF : DataFrame, productsDF : DataFrame, orderProductsDF : DataFrame, outputRootPath : String) =
    {
        Console.println("getting products per department ordered once from customers report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentOrderProducts temp view....")
        val departmentOrderProductsDF = departmentsDF.join(productsDF, "department_id").join(orderProductsDF, "product_id")
        departmentOrderProductsDF.createOrReplaceTempView("departmentOrderProducts")

        val cmd = "select department as Department_Name, product_id as Product_ID, product_name as Product_Name " +
          " from departmentOrderProducts where reorderd = 0 " +
          " group by department, product_id, product_name " +
          " order by department, product_name "

        Console.println(cmd)
        val result = spark.sql(cmd)

        Console.println("showing productsPerDepartmentOrderedOnceFromCustomers report....")
        result.show()

        Console.println("exporting productsPerDepartmentOrderedOnceFromCustomers report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("productsPerDepartmentOrderedOnceFromCustomers"))
    }

     def getMostOrderedProductsPerDepartment(departmentsDF : DataFrame, productsDF : DataFrame, orderProductsDF : DataFrame, outputRootPath : String) =
    {
        Console.println("getting most ordered products per department report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentOrderProducts temp view....")

        val departmentOrderProductsDF = departmentsDF.join(productsDF, "department_id").join(orderProductsDF, "product_id")
        departmentOrderProductsDF.createOrReplaceTempView("departmentOrderProducts")

        val cmd1 = " select department, product_name, count(*) as ordered_count " +
          " from departmentOrderProducts " +
          " where reorderd = 1 " +
          " group by department, product_name " 

        Console.println(cmd1)

        Console.println("create departmentOrderProductsCounts temp view....")

        val result1 = spark.sql(cmd1)
        result1.show();
        result1.createOrReplaceTempView("departmentOrderProductsCounts")

        val cmd2 = " select department as Department_Name, product_name as Product_Name, ordered_count as Max_Times_Ordered " +
          " from departmentOrderProductsCounts a " +
          " where ordered_count = ( " +
          " select max(ordered_count) from departmentOrderProductsCounts b where a.department = b.department  " +
          " ) " +
          " order by department " 

        Console.println(cmd2)
        val result2 = spark.sql(cmd2)

        Console.println("showing mostOrderedProductsPerDepartment report....")
        result2.show()

        Console.println("exporting mostOrderedProductsPerDepartment report....")
        result2.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("mostOrderedProductsPerDepartment"))
    }

    

    // define schemas
    def aisles() : StructType = {
        return StructType(Array(
            StructField("aisle_id", IntegerType), 
            StructField("aisle", StringType)
            ))
        }
    
    def departments() : StructType = {
        return StructType(Array(
            StructField("department_id", IntegerType), 
            StructField("department", StringType)
            ))
        }

    def products() : StructType = {
    return StructType(Array(
        StructField("product_id", IntegerType), 
        StructField("product_name", StringType),
        StructField("aisle_id", IntegerType), 
        StructField("department_id", IntegerType)
        ))
    }

    def orders() : StructType = {
    return StructType(Array(
        StructField("order_id", IntegerType), 
        StructField("user_id", IntegerType), 
        StructField("order_dow", IntegerType),
        StructField("order_hour_of_day", IntegerType)
        ))
    }

    def orderProducts() : StructType = {
    return StructType(Array(
        StructField("order_id", IntegerType), 
        StructField("product_id", IntegerType), 
        StructField("add_to_cart_order", IntegerType), 
        StructField("reorderd", IntegerType)
        ))
    }


}