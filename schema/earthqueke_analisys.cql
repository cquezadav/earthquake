CREATE KEYSPACE earthquakes_analisys WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}; 

CREATE TABLE earthquakes_analisys.earthquakes_raw_data (  
   year text,
   month text,
   timestamp timestamp,
   date text,
   hour text,
   unix_timestamp bigint,
   id text,
   state text,
   city text,
   place text,
   longitude double,
   latitude double,
   magnitude double,
   magnitude_type text,
   tsunami boolean,
   earthquake_type text,
   alert text,
   significant_level int,
   network text,
   PRIMARY KEY ((year, month), timestamp)
)
WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE earthquakes_analisys.earthquakes_count_by_hour (
   state text,
   city text,
   date text,
   hour text,
   count int,
   PRIMARY KEY ((state, city), date, hour)
)
WITH CLUSTERING ORDER BY (date DESC, hour DESC);


CREATE TABLE earthquakes_analisys.earthquakes_magnitude_avg_by_day (
   state text,
   city text,
   date text,
   average double,
   PRIMARY KEY ((state, city), date)
)
WITH CLUSTERING ORDER BY (date DESC);
