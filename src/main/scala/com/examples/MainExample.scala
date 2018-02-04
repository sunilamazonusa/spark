package com.examples


import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, lit}
import java.util
import org.apache.spark.sql.types._
import scala.math.{BigDecimal, BigInt}
import scala.io.Source
import org.apache.spark.sql.types.{
  StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._
import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.util.Properties
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.{Level, LogManager}
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount
import org.apache.spark.sql.SparkSession



object MainExample {

  def main(args: Array[String]): Unit =  {
    /*spark-submit --class com.examples.MainExample spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
     spark-submit --class com.examples.MainExample  --master yarn-cluster 'Jar_Name'*/
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    print("Hi Sunil")
    
    
/*    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
*/    val stopWatch = new StopWatch
    stopWatch.start()
    val debugLogSeparator = "*************************************"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val s3_file = args(0)
    val s3_schema = args(1)
    val s3_del = args(2)
    
    val spark = SparkSession
        .builder()
        .appName(
            "This is created by sunil")
        .config("spark.sql.warehouse.dir","/tmp/spark-warehouse")
        .getOrCreate()
    

object delimgen {
      
 def getDelimiter(datasetDelimiter: String): String = {
   if (datasetDelimiter.length() > 1 ) {
     datasetDelimiter match {
       case "U0001" | "u0001" => "\u0001"
       case _ =>
       throw new IllegalArgumentException(s"Unsupported special chaacter for delimiter: ${datasetDelimiter}")
     }
   }
     else {
       datasetDelimiter;
     }
 }
    }
def getSparkSqlType(dataTypeStr: String, schemaFieldPrecision:Int, schemaFieldScale: Int): DataType = dataTypeStr.toLowerCase() match {
      case "integer" | "smallint" | "int" | "counter" | "varint" => IntegerType
      case "bigint" | "long"                                     => LongType
      case "timestamp" | "timespampz"                            => TimestampType
      case "numeric" | "real" | "fload8" | "double"              => DoubleType
      case "decimal"                                             => DecimalType(Math.max(schemaFieldPrecision,0),Math.max(schemaFieldScale,0))
      case "string" | "char" | "text" | "ascii"|"varchar" | "date" => StringType
      case "float"                                                => FloatType
      case "byte"                                                => ByteType
      case "boolean"                                             => BooleanType
    }
 
def getStrutFromSchemaFile(schemaFile : String): StructType = {
      val schemaLines = Source.fromFile(schemaFile).getLines().toList
      val schemaList: util.ArrayList[StructField] = new java.util.ArrayList[StructField]()
      for ( i <- 0 until schemaLines.length) {
        val schemaLine = schemaLines(i).split("\\|")
        val schemaFieldName = schemaLine(0)
        val schemaFieldType = schemaLine(1)
        val schemaFiledNullability = schemaLine(2)
        val schemaFiledDecimalLength = schemaLine(4)
        var schemaFieldPrecision: Int = 0;
        var schemaFieldScale: Int = 0;
        if ( schemaFieldType.toLowerCase() =="decimal") {
          schemaFieldPrecision = schemaFiledDecimalLength.split(".")(0).toInt
          schemaFieldScale = schemaFiledDecimalLength.split(".")(1).toInt
        }
        val sparkSqlType: DataType = getSparkSqlType(schemaFieldType, schemaFieldPrecision, schemaFieldScale)
            schemaList.add(StructField(schemaFieldName, sparkSqlType, nullable = schemaFiledNullability.toBoolean))
      }
      
      val structSchema: StructType = StructType(DataTypes.createStructType(schemaList))
      
      return structSchema;
    }
    
def createDataFramesFromFile(spark: SparkSession, fileLocation: String, schemaFile: String, datasetDelimiter: String): DataFrame ={
  val schema = getStrutFromSchemaFile(schemaFile)
  val datasetDelimiter1 = delimgen.getDelimiter(datasetDelimiter)
  val dataframe = spark.sqlContext.read
    .format("com.databricks.sark.csv")
    .option("delimiter", datasetDelimiter1)
    .option("header", "false")
    .option("mode", "FAILFAST")
    .option("treatEmptyValuesAsNulls", "true")
    .option("nullValue"," ")
    .option("quote","")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .schema(schema)
    .load(fileLocation)
    dataframe
}


val hadoopConf = spark.sparkContext.hadoopConfiguration;
hadoopConf.set("fs.s3.enableServerSideEncryption", "true")
hadoopConf.set("fs.s3a.server-side-encryption-algorithm", "AES256")

/*log.warn("SPark Application id: " + spark.sparkContext.applicationId)
*/
var rf_df =  createDataFramesFromFile(spark, s3_file, s3_schema, s3_del)
rf_df.show()

/*    val hc = new HiveContext(sc)
    val FilterUDF = new Filter()
    hc.sql("CREATE External TABLE IF NOT EXISTS employee(id INT, name STRING, age INT)")
    val result = hc.sql("select * from employee")
    val salg50k = FilterUDF.Filderdata(result)
    val GroupByV1 = FilterUDF.GBC(result)
    val idcheek = result.withColumn("idchk",lit("30"))
		val idcheek1 = result.withColumn("idchk", FilterUDF.idchk(result.col("id")))
    result.show()
    salg50k.show()
    GroupByV1.show()
    idcheek.show()*/
    
    
  }
}
