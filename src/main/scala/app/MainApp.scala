package app

import services.FlightAnalyticsServices
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import utils.{CsvUtils, InputUtils}
import java.nio.file.Paths

object MainApp {
  def main(args: Array[String]): Unit = {
    // Load configuration
    val config = ConfigFactory.load()

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName(config.getString("spark.appName"))
      .master(config.getString("spark.master"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Load paths and parameters from config
//    val outputPath = config.getString("csv.outputPath")
    val baseDir = Paths.get("").toAbsolutePath.toString
    val outputPath = s"$baseDir/output"

    val topFlyersOutputPath = s"$outputPath/top_flyers"
    val flightsByMonthOutputPath = s"$outputPath/flights_by_month"
    val longestRunOutputPath = s"$outputPath/longest_run"
    val flownTogetherOutputPath = s"$outputPath/flown_together"

    // Parameters from services section
    val numTopFlyers = config.getInt("services.topFrequentFlyer")
    val skipCountry = config.getString("services.countryToSkip")

    // Question 1
    val flightsByMonth = FlightAnalyticsServices.getFlightsByMonth()
    flightsByMonth.show()
    CsvUtils.writeToCsv(flightsByMonth, flightsByMonthOutputPath)

    // Question 2
    val topFlyers = FlightAnalyticsServices.getNumTopFlyer(numTopFlyers)
    topFlyers.show()
    CsvUtils.writeToCsv(topFlyers, topFlyersOutputPath)

    // Question 3
    val longestRun = FlightAnalyticsServices.getLongestRunWithoutSpecificCountry(skipCountry)
    longestRun.show()
    CsvUtils.writeToCsv(longestRun, longestRunOutputPath)

    // Accept user input for Question 4
    InputUtils.getFlownTogetherInputs match {
      case Some((minFlightsTogether, flownTogetherFromDate, flownTogetherToDate)) =>
        // Question 4 with Extra
        val flownTogetherResults = FlightAnalyticsServices.flownTogether(minFlightsTogether, flownTogetherFromDate, flownTogetherToDate)
        flownTogetherResults.show()
        CsvUtils.writeToCsv(flownTogetherResults, flownTogetherOutputPath)

      case None =>
        println("Invalid input. Exiting program.")
        return
    }

    println(s"Results saved to: $outputPath")

    spark.stop()
  }
}
