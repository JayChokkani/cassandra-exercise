package kduraj

import com.datastax.driver.core.Cluster

class Cassandra(IP: String) {

  val clusterBuilder = Cluster.builder()
  clusterBuilder.addContactPoint(IP)
  clusterBuilder.withPort(9042)
  // clusterBuilder.withCredentials("admin", "password") // optional
  val cluster = clusterBuilder.build()
  val session = cluster.connect("interview")
  println(session.getCluster().getClusterName + " connection successful\n")
  var batch = "BEGIN BATCH"

  def createFlightsTable(): Unit = {

    session.execute("DROP TABLE IF EXISTS flights")
    Thread.sleep(5000)

    val SQL =
      """
        |CREATE TABLE flights (
        |  ID int PRIMARY KEY,
        |  YEAR int,
        |  DAY_OF_MONTH int,
        |  FL_DATE timestamp,
        |  AIRLINE_ID int,
        |  CARRIER varchar,
        |  FL_NUM int,
        |  ORIGIN_AIRPORT_ID int,
        |  ORIGIN varchar,
        |  ORIGIN_CITY_NAME varchar,
        |  ORIGIN_STATE_ABR varchar,
        |  DEST varchar,
        |  DEST_CITY_NAME varchar,
        |  DEST_STATE_ABR varchar,
        |  DEP_TIME timestamp,
        |  ARR_TIME timestamp,
        |  ACTUAL_ELAPSED_TIME timestamp,
        |  AIR_TIME timestamp,
        |  DISTANCE int
        |);
      """.stripMargin
    session.execute(SQL).all()

  }

  /*
  CREATE TABLE flights (
    ID int PRIMARY KEY,
    YEAR int,
    DAY_OF_MONTH int,
    FL_DATE timestamp,
    AIRLINE_ID int,
    CARRIER varchar,
    FL_NUM int,
    ORIGIN_AIRPORT_ID int,
    ORIGIN varchar,
    ORIGIN_CITY_NAME varchar,
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

  def insertFlights(flight: FlightPath, counter: Int): Unit = {

    val SQL =
      s"""
         |INSERT INTO flights ( ID, YEAR, DAY_OF_MONTH, FL_DATE, AIRLINE_ID, CARRIER, FL_NUM, ORIGIN_AIRPORT_ID,
         |  ORIGIN, ORIGIN_CITY_NAME,  ORIGIN_STATE_ABR, DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEP_TIME,
         |  ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE )  VALUES (
         |  ${flight.ID},
         |  ${flight.YEAR},
         |  ${flight.DAY_OF_MONTH},
         | '${flight.FL_DATE}',
         |  ${flight.AIRLINE_ID},
         | '${flight.CARRIER}',
         |  ${flight.FL_NUM},
         |  ${flight.ORIGIN_AIRPORT_ID},
         | '${flight.ORIGIN}',
         | '${flight.ORIGIN_CITY_NAME}',
         | '${flight.ORIGIN_STATE_ABR}',
         | '${flight.DEST}',
         | '${flight.DEST_CITY_NAME}',
         | '${flight.DEST_STATE_ABR}',
         | '${flight.DEP_TIME}',
         | '${flight.ARR_TIME}',
         | '${flight.ACTUAL_ELAPSED_TIME}',
         | '${flight.AIR_TIME}',
         |  ${flight.DISTANCE}
         |  );
         |
      """.stripMargin

    //    println("\n" + counter + " " + SQL)
    //    session.execute(SQL)

    batch += SQL
    if ((counter % 1000) == 0) {
      println("\n" + counter + " " + SQL)
      batch += "APPLY BATCH"
      session.execute(batch)
      batch = "BEGIN BATCH"
    }

  }

  def createFlightArrival(): Unit = {

    session.execute("DROP TABLE IF EXISTS flight_arrival")
    Thread.sleep(5000)

    val SQL =
      """
        |CREATE TABLE flight_arrival (
        |   ARR_TIME_BUCKET text,
        |   ARR_TIME timestamp,
        |   CARRIER text,
        |   ORIGIN varchar,
        |   DEST varchar,
        |   PRIMARY KEY( ARR_TIME_BUCKET, ARR_TIME )
        |) WITH CLUSTERING ORDER BY (ARR_TIME ASC);
      """.stripMargin

    session.execute(SQL).all()

  }
  def insertArrival(arrival: FlightArrival, counter: Int): Unit = {

    val SQL =
      s"""
         |INSERT INTO flight_arrival ( ARR_TIME_BUCKET, ARR_TIME, CARRIER, ORIGIN, DEST )
         | VALUES (
         | '${arrival.ARR_TIME_BUCKET}',
         | '${arrival.ARR_TIME}',
         | '${arrival.CARRIER}',
         | '${arrival.ORIGIN}',
         | '${arrival.DEST}'
         | );
         |
      """.stripMargin

    //    println("\n" + counter + " " + SQL)
    //    session.execute(SQL)

    batch += SQL
    if ((counter % 1000) == 0) {
      println("\n" + counter + " " + SQL)
      batch += "APPLY BATCH"
      session.execute(batch)
      batch = "BEGIN BATCH"
    }

  }

  def createFlightNumber(): Unit = {

    session.execute("DROP TABLE IF EXISTS flight_number")
    Thread.sleep(5000)

    val SQL =
      """
        |CREATE TABLE flight_number (
        |  ID int PRIMARY KEY,
        |  CARRIER varchar,
        |  AIRLINE_ID int,
        |  ORIGIN varchar,
        |  ORIGIN_STATE_ABR varchar,
        |  DEST varchar,
        |  DEST_CITY_NAME varchar,
        |  DEST_STATE_ABR varchar,
        |  DEP_TIME timestamp,
        |  ARR_TIME timestamp,
        |  ACTUAL_ELAPSED_TIME timestamp,
        |  AIR_TIME timestamp,
        |  DISTANCE int
        |
        |);
      """.stripMargin

    session.execute(SQL)
  }

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
  def insertFlightsNumber(flight: FlightNumber, counter: Int): Unit = {

    val SQL =
      s"""
         |INSERT INTO flight_number (
         |    ID, CARRIER, AIRLINE_ID, ORIGIN, ORIGIN_STATE_ABR,
         |    DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEP_TIME, ARR_TIME,
         |    ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE ) VALUES (
         |  ${flight.ID},
         | '${flight.CARRIER}',
         |  ${flight.AIRLINE_ID},
         | '${flight.ORIGIN}',
         | '${flight.ORIGIN_STATE_ABR}',
         | '${flight.DEST}',
         | '${flight.DEST_CITY_NAME}',
         | '${flight.DEST_STATE_ABR}',
         | '${flight.DEP_TIME}',
         | '${flight.ARR_TIME}',
         | '${flight.ACTUAL_ELAPSED_TIME}',
         | '${flight.AIR_TIME}',
         |  ${flight.DISTANCE}
         |  );
         |
      """.stripMargin

    //    println("\n" + counter + " " + SQL)
    //    session.execute(SQL)

    batch += SQL
    if ((counter % 1000) == 0) {
      println("\n" + counter + " " + SQL)
      batch += "APPLY BATCH"
      session.execute(batch)
      batch = "BEGIN BATCH"
    }

  }

  def selectQuery(): Unit = {

    System.out.println("system.local: " + session.execute("select now() from system.local").one().getUUID(0) + "\n");

    val list = session.execute("SELECT * FROM kevin.test").all()

    val size = list.size()
    var counter = 0
    for (x <- 0 to size - 1) {
      println(counter + ":\t"
        + list.get(x).getString("id") + "\t"
        + list.get(x).getString("first_name") + "\t"
        + list.get(x).getString("last_name"))
      counter += 1
    }
  }

  def disconnect(str: String) {

    batch += "APPLY BATCH"
    println(batch)
    session.execute(batch)

    cluster.close()
    session.close()
    println("\n" + str + " successfully disconnected")
  }

}

