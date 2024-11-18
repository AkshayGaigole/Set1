import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, sum, when}

object q2 {

  def main(args: Array[String]):Unit={
   /* Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
      based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
      50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
    sales aggregated by performance status*/

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.master", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")



//    val sales_df2=sales.withColumn("performance_status",
//      when(col("total_sales")>50000,"Excellent")
//        .when(col("total_sales")>=25000 && col("total_Sales")<=50000,"Good")
//      .otherwise("Needs Improvement"))
//
//
//    val sales_df3=sales_df2.select(initcap(col("name")),col("total_Sales"),col("performance_status"))
//    sales_df3.show()
//
//    sales_df3.groupBy("performance_status").agg(sum("total_sales")).as("agg_total_Sales").show()


    val sales_df4=sales.select(initcap(col("name")) as("name"),col("total_Sales"),when(col("total_sales")>50000,"Excellent")
      .when(col("total_sales")>=25000 && col("total_Sales")<=50000,"Good")
      .otherwise("Needs Improvement").as("performance_status"))

    sales_df4.show()
    sales_df4.groupBy("performance_status").agg(sum("total_sales")).as("agg_total_Sales").show()




  }

}
