package utils

import scala.io.StdIn.readLine
import scala.util.Try
import java.sql.Date

object InputUtils {

  // Read integer from input
  def readIntFromUser(prompt: String): Option[Int] = {
    println(prompt)
    Try(readLine().toInt).toOption
  }

  // Read date from input
  def readDateFromUser(prompt: String): Option[Date] = {
    println(prompt)
    Try(Date.valueOf(readLine())).toOption
  }

  // Read user input with validation
  def getMinFlightsTogether: Option[Int] = readIntFromUser("Enter minimum number of flights together:")

  def getFlownTogetherFromDate: Option[Date] = readDateFromUser("Enter start date for flown together (yyyy-MM-dd):")

  def getFlownTogetherToDate: Option[Date] = readDateFromUser("Enter end date for flown together (yyyy-MM-dd):")

  // Function to get all inputs for Question 4
  def getFlownTogetherInputs: Option[(Int, Date, Date)] = {
    for {
      minFlightsTogether <- getMinFlightsTogether
      flownTogetherFromDate <- getFlownTogetherFromDate
      flownTogetherToDate <- getFlownTogetherToDate
    } yield (minFlightsTogether, flownTogetherFromDate, flownTogetherToDate)
  }
}
