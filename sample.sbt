name := "AP Project"
 
version := "1.0"
 
scalaVersion := "2.11.8"
 
javaOptions in run += "-Xms4g -Xmx9g -Xss16m"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

libraryDependencies ++= {
  Seq(

    "org.apache.spark"              %%  "spark-core"                % "2.1.0"       % "provided",
    "org.apache.spark"              %%  "spark-mllib"               % "2.1.0"       % "provided",
    "org.apache.spark"              %%  "spark-graphx"              % "2.1.0"       % "provided",
    "org.apache.spark"              %%  "spark-sql"                 % "2.1.0"       % "provided",
    "org.apache.spark"              %%  "spark-hive" 		    % "2.1.0"       % "provided",
    "org.apache.spark"              %%  "spark-streaming"           % "2.1.0"       % "provided"
  )
}
