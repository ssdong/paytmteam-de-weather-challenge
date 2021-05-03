/**
*  Weather Challenage
*  How to quickly verify the implementation:
*  1. Spin up a local spark environment(I used spark-3.0.1) with ./bin/spark-shell
*  2. In the console, type: 
*     :load <absolute_path_to WeatherApp.scala>
*     WeatherApp.main(Array())
*
*     You should be able to see the printed out messages as following:
*     
*     Which country had the hottest average mean temperature over the year? Answer: DJIBOUTI
*     Which country had the most consecutive days of tornadoes/funnel cloud formations? Answer: CANADA
*     Which country had the second highest average mean wind speed over the year? Answer: ARUBA
*
*  Miscellaneous: just make sure you download the zip and throw it within "Downloads". The only limitation that I had coded in the solution
*  is to look for the data file within "Downloads". Also, the actual weather data file is kinda large so I didn't update it. Please copy and
*  paste in the folder under "data/2019" before you run and verify this script.
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import Math.max

object WeatherApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    // Some column names from the data file..
    val countryFullNameCol = "COUNTRY_FULL"
    val meanTemperatureCol = "TEMP"
    val dateCol = "YEARMODA"
    val meanWindSpeedCol = "WDSP"

    // Defining weather schema since "inferSchema" will interpret FRSHTT as Integer type
    // Particularly, in this implementation, we want it to be a String type
    val weatherSchema = StructType(Array(
      StructField("STN---", IntegerType, true),
      StructField("WBAN", IntegerType, true),
      StructField("YEARMODA", IntegerType, true),
      StructField("TEMP", DoubleType, true),
      StructField("DEWP", DoubleType, true),
      StructField("SLP", DoubleType, true),
      StructField("STP", DoubleType, true),
      StructField("VISIB", DoubleType, true),
      StructField("WDSP", DoubleType, true),
      StructField("MXSPD", DoubleType, true),
      StructField("GUST", DoubleType, true),
      StructField("MAX", DoubleType, true),
      StructField("MIN", DoubleType, true),
      StructField("PRCP", DoubleType, true),
      StructField("SNDP", DoubleType, true),
      StructField("FRSHTT", StringType, true))
    )

    val countryListDataPath = "~/Downloads/paytmteam-de-weather-challenge/countrylist.csv".replaceFirst("^~", System.getProperty("user.home"))
    val stationListDataPath = "~/Downloads/paytmteam-de-weather-challenge/stationlist.csv".replaceFirst("^~", System.getProperty("user.home"))
    val weatherDataPath = "~/Downloads/paytmteam-de-weather-challenge/data/2019/*".replaceFirst("^~", System.getProperty("user.home"))

    val countryList = spark.read.option("header", "true").csv(countryListDataPath)
    val stationList = spark.read.option("header", "true").csv(stationListDataPath)
    val weather = spark.read.option("header", "true").schema(weatherSchema).csv(weatherDataPath).withColumnRenamed("STN---", "STN_NO") // Rename STN--- to STN_NO

    // Setting up data: join all country list, station list and weather data.
    val countryWithStationAndWeather = countryList.join(stationList, "COUNTRY_ABBR").join(weather, "STN_NO")
    countryWithStationAndWeather.createOrReplaceTempView("weather")

    // Question 1
    // Strip out the default 9999.9. Not consideirng them as 0 as they would lower the average temperature
    val avgMeanTempWithCountries = spark.sql(s"SELECT $countryFullNameCol, $meanTemperatureCol FROM weather WHERE $meanTemperatureCol != 9999.9")
    // Aggreate by country full name and run order-by in descending manner to get the highest average mean temperature
    val countryWithHottestAvgMeanTemp: String = avgMeanTempWithCountries.groupBy(countryFullNameCol).mean(meanTemperatureCol).orderBy(desc(s"avg($meanTemperatureCol)")).head().get(0).asInstanceOf[String]

    // Question 2
    val tornadoWeather = countryWithStationAndWeather.filter { row =>
          row match {
            case Row(stationNum, tail @ _*) => tail.last.asInstanceOf[String].last == '1'
          }
        }
        .groupBy(countryFullNameCol)
        .agg(collect_list(dateCol))
        .as("dates")
        .map(row => {
          val country = row.getString(0)
          val datesOfTornadoes = row.getAs[Seq[Int]](1)

          // Sort in ascending manner and strip out the duplicate dates
          // Duplicate date with the same weather condition(tornado/funnel cloud) will confuse the computation
          // Particularly, if there exists one station which captures tornado on a specific date, that date is a "tornado" date.
          val sortedDatesOfTornadoes: Seq[Int] = datesOfTornadoes.sortWith(_ < _).distinct

          val format = DateTimeFormatter.ofPattern("yyyyMMdd")
          val len = sortedDatesOfTornadoes.size
          var maxConsecutive = 1
          var counter = 1

          // Iterating through the array of tornado dates and find the longest consecutive dates
          for (index <- 0 to len - 1) {
            if (index != len - 1) {
              val currentDate = LocalDate.parse(sortedDatesOfTornadoes(index).toString, format)
              val nextDate = LocalDate.parse(sortedDatesOfTornadoes(index + 1).toString, format)

              if (ChronoUnit.DAYS.between(currentDate, nextDate) == 1) {
                counter += 1
                maxConsecutive = max(maxConsecutive, counter)
              } else {
                maxConsecutive = max(maxConsecutive, counter)
                counter = 1
              }
            }
          }

          (country, maxConsecutive)
        })
        .toDF("country", "max_consecutive_days")

    val countryWithMostConsecutiveDaysOfTornado: String = tornadoWeather.orderBy(desc("max_consecutive_days")).head().get(0).asInstanceOf[String]

    // Question 3
    // Strip out the default 999.9. Not consideirng them as 0 as they would lower the average wind speed
    val avgMeanWindSpeedWithCountries = spark.sql(s"SELECT $countryFullNameCol, $meanWindSpeedCol FROM weather WHERE $meanWindSpeedCol != 999.9")
    // Aggreate by country full name and run order-by in descending manner and take the first two. The second one is the country which has the second highest average mean wind speed
    val countryWithSecondHighestAvgMeanWindSpeed: String = avgMeanWindSpeedWithCountries.groupBy(countryFullNameCol).mean(meanWindSpeedCol).orderBy(desc(s"avg($meanWindSpeedCol)")).take(2)(1).get(0).asInstanceOf[String]


    // Print the answer
    println(s"Which country had the hottest average mean temperature over the year? Answer: $countryWithHottestAvgMeanTemp") // Answer: DJIBOUTI
    println(s"Which country had the most consecutive days of tornadoes/funnel cloud formations? Answer: $countryWithMostConsecutiveDaysOfTornado") // Answer: CANADA
    println(s"Which country had the second highest average mean wind speed over the year? Answer: $countryWithSecondHighestAvgMeanWindSpeed") // Answer: ARUBA
  }
}