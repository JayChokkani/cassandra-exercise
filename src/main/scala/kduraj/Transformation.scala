package kduraj

case class FlightPath(
  ID: Int,
  YEAR: Int,
  DAY_OF_MONTH: Int,
  FL_DATE: String,
  AIRLINE_ID: Int,
  CARRIER: String,
  FL_NUM: Int,
  ORIGIN_AIRPORT_ID: Int,
  ORIGIN: String,
  ORIGIN_CITY_NAME: String,
  ORIGIN_STATE_ABR: String,
  DEST: String,
  DEST_CITY_NAME: String,
  DEST_STATE_ABR: String,
  DEP_TIME: String,
  ARR_TIME: String,
  ACTUAL_ELAPSED_TIME: String,
  AIR_TIME: String,
  DISTANCE: Int)

case class FlightNumber(
  ID: Int,
  CARRIER: String,
  AIRLINE_ID: Int,
  ORIGIN: String,
  ORIGIN_STATE_ABR: String,
  DEST: String,
  DEST_CITY_NAME: String,
  DEST_STATE_ABR: String,
  DEP_TIME: String,
  ARR_TIME: String,
  ACTUAL_ELAPSED_TIME: String,
  AIR_TIME: String,
  DISTANCE: Int)

case class FlightArrival(
  ARR_TIME_BUCKET: String,
  ARR_TIME: String,
  CARRIER: String,
  ORIGIN: String,
  DEST: String)

/**
 * Data Transformation for faulty free inserts into Cassandra
 */
class Transformation {

  def transformTime(time: String): List[String] = {

    var min = "00"
    var hour = "00"

    if (time == "2400") {
      hour = "00"; min = "00"
    } else {
      if (time.length == 4) {
        hour = time.substring(0, time.length - 2)
        min = time.substring(time.length - 2)
      } else if (time.length == 3) {
        hour = "0" + time.substring(0, time.length - 2)
        min = time.substring(time.length - 2)
      } else if (time.length == 2) {
        hour = "00"
        min = time
      } else if (time.length == 1) {
        hour = "00"
        min = "0" + time
      }
    }

    List(hour, min)
  }

  def transformMinutes2Time(elapsed: Int): String = {

    var date = ""
    // time in minutes must be minimum 60 to avoid division by 0
    if (elapsed > 59) {
      val hour = elapsed / 60
      val min = elapsed % (hour * 60)
      date = s"1970-01-01 ${hour}:${min}:00"
    } else {
      date = s"1970-01-01 00:${elapsed}:00"
    }
    return date
  }

  def test(): Unit = {
    println(transformTime("2400"))
    // println(transformTime("1234"))
    // println(transformTime("123"))
    // println(transformTime("12"))
    // println(transformTime("1"))

  }

}

