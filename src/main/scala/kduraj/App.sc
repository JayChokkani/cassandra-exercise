def transformTime(time: String): List[String] = {

  var min = "00"
  var hour = "00"

  if (time == "2400") { hour = "00"; min = "00" }
  else {
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

transformTime("3")


var hour = "2"
"%02d".format(hour.toInt)
hour = "%02d".format(hour.toInt)

var min = "2"
min = "%02d".format(min.toInt)

val arr_time = s"2012-01-17 ${hour}:${min}:00"

arr_time.substring(0,15)


