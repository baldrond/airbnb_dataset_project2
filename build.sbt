name := "airbnb_dataset_project2"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in Compile := Some("alternative_listings")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"
