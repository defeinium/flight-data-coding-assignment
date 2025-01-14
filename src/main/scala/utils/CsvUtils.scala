package utils

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}

object CsvUtils {

  // Read a CSV file into a Dataset
  def readCsv[T](spark: SparkSession, filePath: String, delimiter: String, hasHeader: Boolean, inferSchema: Boolean)(implicit encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] = {
    try{

      val df: DataFrame = spark.read
        .option("header", hasHeader)
        .option("delimiter", delimiter)
        .option("inferSchema", inferSchema)
        .csv(filePath)

      // Convert the DataFrame to a Dataset of type T using the implicit encoder
      df.as[T]
    } catch{
      case e: Exception =>
        throw new RuntimeException(s"Error reading CSV file: ${e.getMessage}", e)
    }
  }

  // write Dataset into csv file
  def writeToCsv(data: Dataset[_], outputPath: String): Unit = {
    data.repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(s"$outputPath")
  }

}
