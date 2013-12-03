name := "Tinkoff Database"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "commons-dbutils" % "commons-dbutils" % "1.5",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test"
)
