Cassandra Exercise
==================

------------------------------------------------------------------------------------------------------------------------
3. Load the source data from the flights_from_pg.csv file into the flights table using a DataStax
------------------------------------------------------------------------------------------------------------------------
```
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
```

###a.	How many records loaded?
   * Total Records Inserted into flights        : 1,048,576
   * Total Records Inserted into flight_airport :   406,469 
   * Total Records Inserted into flight_number  : 1,048,576  
   * Total Records Inserted into flight_arrival : 1,048,576
   
###b.	Were any errors returned during data loading?
   * Replace Date Format 2015/10/28 => 2015-10-25
   * Transform value 366 => 3 hours and 66 minutes => 4 hours and 6 minutes
   * Time is specify as "2400" => "0000"

###c.	Please provide your code base


------------------------------------------------------------------------------------------------------------------------
4. Create and populate two Cassandra “query” tables to answer the following questions:)
------------------------------------------------------------------------------------------------------------------------

###a) Build a query table to list all flights leaving a particular airport, sorted by time.

```
   CREATE TABLE flight_airport (
      ORIGIN varchar,
      DEP_TIME timestamp,
      carrier text,
      fl_num int,
      PRIMARY KEY( ORIGIN, DEP_TIME )
   ) WITH CLUSTERING ORDER BY (DEP_TIME ASC);

   csc.sql("INSERT INTO TABLE interview.flight_airport SELECT origin, dep_time, carrier, fl_num FROM interview.flights").collect();
   SELECT * FROM interview.flight_airport WHERE origin = 'LAX' LIMIT 40;
      
 origin | dep_time                 | fl_num | carrier
--------+--------------------------+--------+---------
    LAX | 2012-01-01 08:00:00+0000 |    204 |      AA
    LAX | 2012-01-01 08:14:00+0000 |   2268 |      DL
    LAX | 2012-01-01 08:17:00+0000 |   2400 |      AA
    LAX | 2012-01-01 08:46:00+0000 |   1651 |      UA
    LAX | 2012-01-01 08:57:00+0000 |   1614 |      UA
    LAX | 2012-01-01 08:58:00+0000 |     30 |      AA
    LAX | 2012-01-01 08:58:00+0000 |   2404 |      AA
    LAX | 2012-01-01 09:11:00+0000 |    364 |      UA
    LAX | 2012-01-01 09:11:00+0000 |    510 |      F9
    LAX | 2012-01-01 09:11:00+0000 |   3063 |      MQ

(10 rows)
```

### b.)	List the carrier, origin, and destination airport for a flight based on 10 minute buckets of arr_time.

```
CREATE TABLE interview.flight_arrival (
   ARR_TIME_BUCKET text,
   ARR_TIME timestamp,
   CARRIER text,
   ORIGIN varchar,
   DEST varchar,
   PRIMARY KEY( ARR_TIME_BUCKET, ARR_TIME )
) WITH CLUSTERING ORDER BY (ARR_TIME ASC);
```

### cqlsh:interview> SELECT carrier, origin, dest, arr_time FROM flight_arrival WHERE arr_time_bucket = '2012-01-10 18:1';

```
 carrier | origin | dest | arr_time
---------+--------+------+--------------------------
      AA |    LAX |  STL | 2012-01-11 02:10:00+0000
      AS |    SEA |  BUR | 2012-01-11 02:11:00+0000
      AA |    DFW |  LAX | 2012-01-11 02:12:00+0000
      AA |    LGA |  DFW | 2012-01-11 02:13:00+0000
      AS |    PSP |  SEA | 2012-01-11 02:14:00+0000
      AA |    ORD |  DFW | 2012-01-11 02:15:00+0000
      AA |    MIA |  DFW | 2012-01-11 02:16:00+0000
      AS |    SNA |  SEA | 2012-01-11 02:17:00+0000
      B6 |    AUS |  LGB | 2012-01-11 02:18:00+0000
      YV |    ORD |  MSN | 2012-01-11 02:19:00+0000

```
------------------------------------------------------------------------------------------------------------------------
5. Exercise the following queries using either Search or Analytics
------------------------------------------------------------------------------------------------------------------------

a.	How many flights originated from the ‘HNL’ airport code on 2012-01-25
### scala> csc.sql("SELECT count(*) HNL FROM interview.flights WHERE origin = 'HNL' AND dep_time LIKE '2012-01-25%'").show
```
 +---+
 |HNL|
 +---+
 |288|
 +---+
```

### b.	How many airport codes start with the letter ‘A’

```
scala> csc.sql("SELECT origin FROM interview.flights WHERE origin LIKE 'A%' UNION DISTINCT SELECT dest FROM interview.flights WHERE dest LIKE 'A%'").collect()
res2: Array[org.apache.spark.sql.Row] = Array([ADK], [ADQ], [ALB], [ART], [AEX], [ASE], [AMA], [AZO], [ATL], [AGS], [ANC], [ATW], [ABE], [AUS], [ABI], [ABQ], [ABY], [AVL], [AVP], [ACT], [ACV], [ACY])
```

###c.	What originating airport had the most flights on 2012-01-23
```
scala> csc.sql("SELECT DISTINCT origin, count(origin) total FROM interview.flights WHERE dep_time like '2012-01-23%' GROUP BY origin ORDER BY total DESC limit 1").show()
+------+-----+
|origin|total|
+------+-----+
|   ATL| 2155|
+------+-----+
```
------------------------------------------------------------------------------------------------------------------------
6. Bonus – make a batch update to all records with a ‘BOS’ airport code using Spark and change the airport code to ‘TST’
------------------------------------------------------------------------------------------------------------------------
```
sc.cassandraTable("interview", "flights").update("origin").with(set("TST")).where(eq("BOS"));

BEGIN BATCH
UPDATE interview.flights SET origin = 'TST' WHERE origin='BOS';
UPDATE interview.flights SET dest = 'TST' WHERE dest='BOS';
APPLY BATCH;
```
```
csc.sql("BEGIN BATCH UPDATE interview.flights SET origin = 'TST' WHERE origin='BOS'; UPDATE interview.flights SET dest = 'TST' WHERE dest='BOS'; APPLY BATCH;");
csc.sql("BEGIN BATCH UPDATE interview.flights SET origin = 'TST' WHERE origin='BOS'; APPLY BATCH;");
csc.sql("BEGIN BATCH; UPDATE interview.flights SET origin = 'TST' WHERE origin='BOS'; APPLY BATCH;");
sc.sqlContext.sql("UPDATE interview.flights SET origin = 'TST' WHERE origin='BOS'");
```
