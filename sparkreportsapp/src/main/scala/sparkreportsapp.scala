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

        //handle input agrs
        var runMode: String = "";
        if (args.length > 0) {
            runMode = args(0)
        }
        Console.println("Start program with runMode=" + runMode)

        //start spark
        initiateSparkSession()
        
        //resolve input args
        runMode match {
            case "1"  => getReport_1()
            case "2"  => getReport_2()
            case "3"  => getReport_3()
            case "4"  => getReport_4()
            case "5"  => getReport_5()
            case "all"  => getAllReports()
            case _  => println("Invalid Argument. No reports running.")
            }

        //stop spark
        endSparkSession()

        Console.println("End program")
    }

    // get report methods
    def getReport_1()={
        val departmentsDF = loadDataFrame(departments(), datasetRootPath.concat("departments.csv"))
        val productsDF = loadDataFrame(products(), datasetRootPath.concat("products.csv"))

        getTotalProductsPerDepartmentReport(departmentsDF, productsDF)
    }

    def getReport_2()={
        val ordersDF = loadDataFrame(orders(), datasetRootPath.concat("orders.csv"))

        getTotalOrdersPerWeekdayReport(ordersDF)
    }

    def getReport_3()={
        val departmentsDF = loadDataFrame(departments(), datasetRootPath.concat("departments.csv"))
        val productsDF = loadDataFrame(products(), datasetRootPath.concat("products.csv"))
        val orderProductsDF = loadDataFrame(orderProducts(), datasetRootPath.concat("order_products.csv"))

        getProductsPerDepartmentOrderedOnceFromCustomers(departmentsDF, productsDF, orderProductsDF)
    }

    def getReport_4()={
        val departmentsDF = loadDataFrame(departments(), datasetRootPath.concat("departments.csv"))
        val productsDF = loadDataFrame(products(), datasetRootPath.concat("products.csv"))
        val orderProductsDF = loadDataFrame(orderProducts(), datasetRootPath.concat("order_products.csv"))

        getMostOrderedProductsPerDepartment(departmentsDF, productsDF, orderProductsDF)
    }

    def getReport_5()={
        val aislesDF = loadDataFrame(aisles(), datasetRootPath.concat("aisles.csv"))
        val productsDF = loadDataFrame(products(), datasetRootPath.concat("products.csv"))
        val orderProductsDF = loadDataFrame(orderProducts(), datasetRootPath.concat("order_products.csv"))

        getFirstOrderedProductsPercentagePerAisle(aislesDF,productsDF,orderProductsDF)
    }

    def getAllReports()={
        getReport_1()
        getReport_2()
        getReport_3()
        getReport_4()
        getReport_5()
    }

    // spark methods
    def initiateSparkSession()=
    {
        Console.println("starting spark....")
        val spark : SparkSession = SparkSession.builder().master("local[*]").appName("Project2").getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("OFF")
    }

    def endSparkSession()=
    {
        Console.println("stopping spark....")
        val spark = SparkSession.builder.getOrCreate()
        spark.stop()
    }

    def loadDataFrame(schema : StructType, path : String):DataFrame = {
        val spark = SparkSession.builder.getOrCreate()
        val df = spark.read.schema(schema).csv(path)
        return df
    }

    def getTotalProductsPerDepartmentReport(departmentsDF : DataFrame, productsDF : DataFrame) =
    {
        Console.println("getting total products per department report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentProducts temp view....")
        val departmentProductsDF = departmentsDF.join(productsDF, "department_id")
        departmentProductsDF.createOrReplaceTempView("departmentProducts")

        var cmd = "select department as Department_Name, count(*) as Products_Count from departmentProducts group by department order by department"
        Console.println(cmd)
        var result = spark.sql(cmd)

        Console.println("showing totalProductsPerDepartment report....")
        result.show()

        Console.println("exporting totalProductsPerDepartment report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("totalProductsPerDepartment"))
    }

    def getTotalOrdersPerWeekdayReport(ordersDF : DataFrame) = {
        Console.println("getting total orders per weekday report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create orders temp view....")
        ordersDF.createOrReplaceTempView("orders")

         var cmd = "select order_dow as Day_Of_Week, count(*) as Orders_Count from orders where order_dow is not null group by order_dow order by order_dow asc"
        Console.println(cmd)
        var result = spark.sql(cmd)

        Console.println("showing totalOrdersPerWeekdayReport report....")
        result.show()

        Console.println("exporting totalOrdersPerWeekdayReport report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("totalOrdersPerWeekdayReport"))

    }

    def getProductsPerDepartmentOrderedOnceFromCustomers(departmentsDF : DataFrame, productsDF : DataFrame, orderProductsDF : DataFrame) =
    {
        Console.println("getting products per department ordered once from customers report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentOrderProducts temp view....")
        val departmentOrderProductsDF = departmentsDF.join(productsDF, "department_id").join(orderProductsDF, "product_id")
        departmentOrderProductsDF.createOrReplaceTempView("departmentOrderProducts")

        var cmd = "select department as Department_Name, product_id as Product_ID, product_name as Product_Name " +
          " from departmentOrderProducts where reorderd = 0 " +
          " group by department, product_id, product_name " +
          " order by department, product_name "

        Console.println(cmd)
        var result = spark.sql(cmd)

        Console.println("showing productsPerDepartmentOrderedOnceFromCustomers report....")
        result.show()

        Console.println("exporting productsPerDepartmentOrderedOnceFromCustomers report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("productsPerDepartmentOrderedOnceFromCustomers"))
    }

     def getMostOrderedProductsPerDepartment(departmentsDF : DataFrame, productsDF : DataFrame, orderProductsDF : DataFrame) =
    {
        Console.println("getting most ordered products per department report....")

        val spark = SparkSession.builder.getOrCreate()

        Console.println("create departmentOrderProducts temp view....")

        val departmentOrderProductsDF = departmentsDF.join(productsDF, "department_id").join(orderProductsDF, "product_id")
        departmentOrderProductsDF.createOrReplaceTempView("departmentOrderProducts")

        var cmd = " select department, product_name, count(*) as ordered_count " +
          " from departmentOrderProducts " +
          " where reorderd = 1 " +
          " group by department, product_name " 

        Console.println(cmd)

        Console.println("create departmentOrderProductsCounts temp view....")

        var result = spark.sql(cmd)
        result.show();
        result.createOrReplaceTempView("departmentOrderProductsCounts")

        cmd = " select department as Department_Name, product_name as Product_Name, ordered_count as Max_Times_Ordered " +
          " from departmentOrderProductsCounts a " +
          " where ordered_count = ( " +
          " select max(ordered_count) from departmentOrderProductsCounts b where a.department = b.department  " +
          " ) " +
          " order by department " 

        Console.println(cmd)
        result = spark.sql(cmd)

        Console.println("showing mostOrderedProductsPerDepartment report....")
        result.show()

        Console.println("exporting mostOrderedProductsPerDepartment report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("mostOrderedProductsPerDepartment"))
    }

    def getFirstOrderedProductsPercentagePerAisle(aislesDF: DataFrame, productsDF : DataFrame, orderProductsDF : DataFrame) =
    {
        Console.println("getting first ordered products percentage per aisle report....")

        val spark = SparkSession.builder.getOrCreate()

        productsDF.createOrReplaceTempView("products")
        var cmd = " select aisle_id, count(*) as total_products from products group by aisle_id " 
        Console.println(cmd)
        var totalProductsPerAisleDF = spark.sql(cmd)
        totalProductsPerAisleDF.show();

        val aisleOrderProductsDF = aislesDF.join(productsDF, "aisle_id").join(orderProductsDF, "product_id").join(totalProductsPerAisleDF, "aisle_id" )
        aisleOrderProductsDF.createOrReplaceTempView("aisleOrderProducts")
        cmd = " select aisle as Aisle_Name, count(distinct product_id) / max(total_products) * 100 as Percentage from  aisleOrderProducts where add_to_cart_order = 1 group by aisle order by aisle " 
        Console.println(cmd)
        var result = spark.sql(cmd)
        result.show();

        Console.println("exporting firstOrderedProductsPercentagePerAisle report....")
        result.write.mode("overwrite").option("header",true).csv(outputRootPath.concat("firstOrderedProductsPercentagePerAisle"))
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