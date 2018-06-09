package kduraj

import scala.io.Source

/**
 * Arrival time
 */
class ArrivalTimes(server: String) extends Transformation {

  /*
CREATE TABLE flight_path (
  00. ID int PRIMARY KEY,
  01. YEAR int,
  02. DAY_OF_MONTH int,
  03. FL_DATE timestamp,
  04, AIRLINE_ID int,
  05. CARRIER varchar,
  06. FL_NUM int,
  07. ORIGIN_AIRPORT_ID int,
  08. ORIGIN varchar,
  09. ORIGIN_CITY_NAME varchar,
  10. ORIGIN_STATE_ABR varchar,
  11. DEST varchar,
  12. DEST_CITY_NAME varchar,
  13. DEST_STATE_ABR varchar,
  14. DEP_TIME timestamp,
  15. ARR_TIME timestamp,
  16. ACTUAL_ELAPSED_TIME timestamp,
  17. AIR_TIME timestamp,
  18. DISTANCE int
);

CREATE TABLE flight_arrival (
   ARR_TIME_BUCKET text,
   ARR_TIME timestamp,
   CARRIER text,
   ORIGIN varchar,
   DEST varchar,
   PRIMARY KEY( ARR_TIME_BUCKET, ARR_TIME )
) WITH CLUSTERING ORDER BY (ARR_TIME ASC);

*/

  def generateArrivalTime(file: String, insert: Boolean = false): Unit = {

    val obj = new Cassandra(server)
    obj.createFlightArrival()
    var counter = 0

    Source.fromFile(file).getLines.foreach { line =>

      val col = line.split(",").map(_.trim)
      val date = col(3).replace('/', '-')

      val departure = transformTime(col(14))
      val arrival = transformTime(col(15))
      val elapsed = transformMinutes2Time(col(16).toInt)
      val airtime = transformMinutes2Time(col(17).toInt)

      val departure0 = date + " " + departure(0) + ":" + departure(1) + ":00"
      val arrival0 = date + " " + arrival(0) + ":" + arrival(1) + ":00"

      if (insert) {
        val flight = new FlightArrival(
          arrival0.substring(0, 15),
          arrival0,
          col(5),
          col(8),
          col(11)
        )

        counter += 1
        obj.insertArrival(flight, counter)

      } else {
        println(
          arrival0.substring(0, 15),
          airtime + "," +
            col(5) + "," +
            col(8) + "," +
            col(11)
        )
      }
    }

    if (insert) obj.disconnect(s"Total Records Inserted into flight_arrival: ${counter}")
  }

}
