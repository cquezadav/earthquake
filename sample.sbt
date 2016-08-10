name := "Sample Project"
 
version := "1.0"
 
scalaVersion := "2.11.7"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

//libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" % "provided"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0" 
//libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
//libraryDependencies += "joda-time" % "joda-time" % "2.9.4"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0"