package kduraj

object Application extends App {

  println("Loading Data Into Cassandra")
  val server = "127.0.0.1"

  //  Keyspace(server, "interview")

  //  val flights = new Flights(server)
  //  flights.flights2Cassandra("data/flights_from_pg.csv", true)

  //  val numbers = new FlightNumbers(server)
  //  numbers.flightsNumber2Cassandra("data/flightNum_from_pg.csv", true)

  val arrival = new ArrivalTimes(server)
  arrival.generateArrivalTime("data/flights_from_pg.csv", true)

}