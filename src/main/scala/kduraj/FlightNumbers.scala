package kduraj

import scala.io.Source

/**
 * Created by kduraj on 10/16/15.
 */
class FlightNumbers(server: String) extends Transformation {
  /*
  CREATE TABLE flight_number (
    ID int PRIMARY KEY,
    CARRIER varchar,
    AIRLINE_ID int,
    ORIGIN varchar,
    ORIGIN_STATE_ABR varchar,
    DEST varchar,
    DEST_CITY_NAME varchar,
    DEST_STATE_ABR varchar,
    DEP_TIME timestamp,
    ARR_TIME timestamp,
    ACTUAL_ELAPSED_TIME timestamp,
    AIR_TIME timestamp,
    DISTANCE int
  );
  */

  def flightsNumber2Cassandra(file: String, insert: Boolean = false): Unit = {

    val obj = new Cassandra(server)
    obj.createFlightNumber()
    var counter = 0

    Source.fromFile(file).getLines.foreach { line =>

      val col = line.split(",").map(_.trim)
      //val date = col(3).replace('/', '-')

      val departure = transformTime(col(8))
      val arrival = transformTime(col(9))
      val elapsed = transformMinutes2Time(col(10).toInt)
      val airtime = transformMinutes2Time(col(11).toInt)

      val departure0 = "1970-01-01 " + departure(0) + ":" + departure(1) + ":00"
      val arrival0 = "1970-01-01 " + arrival(0) + ":" + arrival(1) + ":00"

      if (insert) {
        val flightNum = new FlightNumber(
          col(0).toInt, // ID
          col(1), // CARRIER
          col(2).toInt, // AIRLINE_ID
          col(3), // ORIGIN_STATE_ABR
          col(4), // ORIGIN_STATE_ABR
          col(5), // DEST
          col(6), // DEST_CITY_NAME
          col(7), // DEST_STATE_ABR
          departure0, // DEP_TIME
          arrival0, // ARR_TIME
          elapsed, // ACTUAL_ELAPSED_TIME
          airtime, // AIR_TIME
          col(12).toInt // DISTANCE
        )
        counter += 1
        obj.insertFlightsNumber(flightNum, counter)

      } else {
        println(
          col(0) + "," +
            col(1) + "," +
            col(2) + "," +
            col(3) + "," +
            col(4) + "," +
            col(5) + "," +
            col(6) + "," +
            col(7) + "," +
            departure0 + "," +
            arrival0 + "," +
            elapsed + "," +
            airtime + "," +
            col(12))
      }
    }

    if (insert) obj.disconnect(s"Total Records Inserted into flight_number: ${counter}")
  }

}
