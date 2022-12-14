ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1"
)

lazy val root = (project in file("."))
  .settings(
    name := "Task5"
  )
