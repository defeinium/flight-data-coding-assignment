package services

import com.typesafe.config.ConfigFactory
import models.Input.{flightData, passengers}
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.CsvUtils
import models.Output.{FlightsByMonth, FlownTogether, LongestRunWithoutCountry, NumTopFlyer}
import org.apache.spark.sql.functions.to_date
import java.sql.Date

object FlightAnalyticsServices {

  // Initialize Spark session
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  // Load configuration
  private val config = ConfigFactory.load()

  // Read file paths from config
  private val flightDataPath = config.getString("csv.flightDataPath")
  private val passengerDataPath = config.getString("csv.passengerDataPath")

  // Read the flight and passenger data
  private val flights = CsvUtils.readCsv[flightData](spark, flightDataPath, ",", hasHeader = true, inferSchema = true)
  private val passengers = CsvUtils.readCsv[passengers](spark, passengerDataPath, ",", hasHeader = true, inferSchema = true)

  // Q1: Find the total number of flights for each month
  def getFlightsByMonth(): Dataset[FlightsByMonth] = {
    import flights.sparkSession.implicits._

    // Step 1: Extract the month from date
    val flightsWithMonth = flights.map { flight =>
      val month = flight.date.substring(5, 7).toLong // Extract month from the "YYYY-MM-DD" format
      (month, flight.flightId)
    }

    // Step 2: Group by month, aggregate count distinct flightId
    val groupedByMonth = flightsWithMonth
      .groupByKey { case (month, _) => month } // Group by month
      .mapGroups { case (month, flightRecords) =>
        val distinctFlights = flightRecords.map(_._2).toSet.size // Count distinct flight IDs
        FlightsByMonth(month, distinctFlights) // Map result to `FlightsByMonth`
      }

    groupedByMonth.orderBy($"month")
  }

  // Q2: Find the names of the top n most frequent flyers
  def getNumTopFlyer(numTopFlyer: Int): Dataset[NumTopFlyer] = {
    import flights.sparkSession.implicits._

    // Step 1: Map flights to (passengerId, 1) and count flights
    val passengerFlightCounts = flights
      .map(flight => (flight.passengerId, 1)) // Map to (passengerId, 1)
      .groupByKey(_._1) // Group by passengerId
      .mapGroups { case (passengerId, flights) =>
        val flightCount = flights.size
        (passengerId, flightCount)
      }
      .map { case (passengerId, flightCount) => NumTopFlyer(passengerId, flightCount, "", "") }

    // Step 2: Sort by flight count desc and take the top n Flyers
    val topFlyers = passengerFlightCounts
      .orderBy($"flightCount".desc) // Sort by flight count descending
      .limit(numTopFlyer)

    // Step 3: Join to passengers to get names
    val passengersAlias = passengers.as("p") // Alias for passengers
    val topFlyersAlias = topFlyers.as("tf") // Alias for topFlyers

    val topFlyersWithDetails = topFlyersAlias
      .joinWith(passengersAlias, $"tf.passengerId" === $"p.passengerId")
      .map { case (topFlyer, passenger) =>
        topFlyer.copy(firstName = passenger.firstName, lastName = passenger.lastName)
      }
      .orderBy($"flightCount".desc) // Sort by flight count descending

    topFlyersWithDetails
  }

  // Q3: Find the greatest number of countries a passenger has been in without being in the UK
  def getLongestRunWithoutSpecificCountry(skipCountry: String): Dataset[LongestRunWithoutCountry] = {
    import flights.sparkSession.implicits._

    flights
      .groupByKey(_.passengerId)
      .mapGroups { case (passengerId, flightDataIter) =>
        // Step 1: Sort flight data by date
        val flightData = flightDataIter.toList.sortBy(_.date)

        // Step 2: Combine all visited countries in order into a list
        val countries = flightData.flatMap(fd => List(fd.from, fd.to))

        // Step 3: Track consecutive segments while checking for duplicates
        var segmentList = List[List[String]]() // List to hold the segments
        var currentSegment = List[String]() // Current segment list

        countries.foreach { country =>
          if (country == skipCountry) {
            // If skipCountry is encountered, end current segment
            if (currentSegment.nonEmpty) {
              segmentList = segmentList :+ currentSegment
            }
            currentSegment = List() // and start a new segment
          } else {
            // Check if the current country is the same as the last country in the segment
            if (currentSegment.isEmpty || currentSegment.last != country) {
              // Add country to the current segment only if it is different from the last country in the segment
              currentSegment = currentSegment :+ country
            }
          }
        }

        // Add the last segment if not empty
        if (currentSegment.nonEmpty) {
          segmentList = segmentList :+ currentSegment
        }

        // Step 4: Find the longest segment and its length
        val longestRun = if (segmentList.nonEmpty) segmentList.map(_.length).max else 0

        // DEBUG Purpose: Flatten the segments as a string for output
        // val flattenedTestOutput = segmentList.flatten.mkString(",")

        LongestRunWithoutCountry(passengerId, longestRun)
      }
      .orderBy($"longestRun".desc)
  }

  // Q4: Find the passengers who have been on more than 3 flights together
  // Extra: Find the passengers who have been on more than N flights together within the range (from,to)
  def flownTogether(atLeastNTimes: Int, from: Date, to: Date): Dataset[FlownTogether] = {
    import spark.implicits._

    // Step 1: Filter flights within the given date range
    val filteredFlights = flights
      .withColumn("parsedDate", to_date($"date", "yyyy-MM-dd"))
      .filter($"parsedDate".between(from, to))  // Filter flights within the date range
      .as[flightData]

    // Step 1: Group passengers by flightId and create pairs
    val passengerPairs = filteredFlights
      .groupByKey(_.flightId)  // Group by flightId to find passengers on the same flight
      .flatMapGroups { case (_, flightData) =>
        val passengers = flightData.map(_.passengerId).toList
        for {
          p1 <- passengers
          p2 <- passengers if p1 < p2  // Ensure each pair is counted only once
        } yield (p1, p2)
      }

    // Step 2: Count how many times each passenger pair appears (i.e., how many flights they were on together)
    val flightCount = passengerPairs
      .toDF("passenger1Id", "passenger2Id")
      .groupBy("passenger1Id", "passenger2Id")
      .count()
      .filter($"count" > atLeastNTimes)
      .select("passenger1Id", "passenger2Id", "count")
      .withColumnRenamed("count", "numberOfFlightsTogether")
      .orderBy($"numberOfFlightsTogether".desc)
      .as[FlownTogether]

    flightCount
  }
}
