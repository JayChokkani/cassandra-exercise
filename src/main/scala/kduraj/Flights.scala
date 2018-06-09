package kduraj

import scala.io.Source

/**
 * Created by kduraj on 10/16/15.
 */
class Flights(server: String) extends Transformation {

  def flights2Cassandra(file: String, insert: Boolean = false): Unit = {

    val cassandra = new Cassandra(server)
    cassandra.createFlightsTable()
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
        val flight = new FlightPath(
          col(0).toInt,
          col(1).toInt,
          col(2).toInt,
          date,
          col(4).toInt,
          col(5),
          col(6).toInt,
          col(7).toInt,
          col(8),
          col(9),
          col(10),
          col(11),
          col(12),
          col(13),
          departure0,
          arrival0,
          elapsed,
          airtime,
          col(18).toInt
        )
        counter += 1
        cassandra.insertFlights(flight, counter)

      } else {
        println(
          col(0) + "," +
            col(1) + "," +
            col(2) + "," +
            date + "," +
            col(4) + "," +
            col(5) + "," +
            col(6) + "," +
            col(7) + "," +
            col(8) + "," +
            col(9) + "," +
            col(10) + "," +
            col(11) + "," +
            col(12) + "," +
            col(13) + "," +
            departure0 + "," +
            arrival0 + "," +
            elapsed + "," +
            airtime + "," +
            col(18))
      }
    }

    if (insert) cassandra.disconnect(s"Total Records Inserted flights: ${counter}")
  }

}
