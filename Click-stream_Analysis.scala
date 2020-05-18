// Databricks notebook source
// MAGIC %md
// MAGIC ###Click stream analytics for analyzing user logs on the ecommerce site

// COMMAND ----------

// DBTITLE 1,Importing all libraries
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, from_json}
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Listing all user log files
// MAGIC %fs ls /FileStore/tables/clickstream/user_log_json/

// COMMAND ----------

// DBTITLE 1,Checking few JSON records 
// MAGIC %fs head /FileStore/tables/clickstream/user_log_json/part-00000-tid-3581789376639256587-622f3f56-f87e-49ac-855c-82767280ffee-24-1-c000.json

// COMMAND ----------

// DBTITLE 1,Product category list
//This is load the product category list

val prod_category_df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\t")
  .load("/FileStore/tables/clickstream/urlmap.tsv")

prod_category_df.show()
prod_category_df.createOrReplaceTempView("vw_prod_category")


// COMMAND ----------

// DBTITLE 1,Structured streaming Schema and DataFrame
//Defining the schema
val jsonSchema = new StructType().add("ts", TimestampType)
                                 .add("ip", StringType)
                                 .add("url", StringType)
                                 .add("swid", StringType)
                                 .add("city", StringType)
                                 .add("country", StringType)
                                 .add("state", StringType)

//Streaming DataFrame
val streamingUserLogDF = 
  spark
    .readStream                      
    .schema(jsonSchema)              
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json("/FileStore/tables/clickstream/user_log_json/")

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

// COMMAND ----------

// DBTITLE 1,Windowing Operations using Structured Streaming APIs
//Streaming query
val streamingCountsDF = 
  streamingUserLogDF
    .join(prod_category_df,Seq("url"))
    .groupBy($"category", window($"ts", "24 hours"))
    .count()
    .select($"category", date_format($"window.end", "MMM-dd HH:mm").as("time"), $"count")
    .orderBy("time", "category")

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

display(streamingCountsDF)
Thread.sleep(5000)


// COMMAND ----------

// DBTITLE 1,Query handler for the streaming DF
val query =
  streamingCountsDF
    .writeStream
    .format("memory")        // memory = store in-memory table
    .queryName("counts")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()


