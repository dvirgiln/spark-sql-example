organization := "com.david"
version := "0.1-SNAPSHOT"
name := "spark-sql-example"
val sparkVersion = "2.4.1"
scalaVersion := "2.12.8"

val commonDependencies: Seq[ModuleID] = Seq(
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)

val sparkDependencies  : Seq[ModuleID] = Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion),
  ("org.apache.spark" %% "spark-sql" % sparkVersion)
)


val log4j : Seq[ModuleID] = Seq("log4j" % "log4j" % "1.2.17")

val root = (project in file(".")).
  settings(
    libraryDependencies ++= commonDependencies ++ sparkDependencies,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:_"
    )
  )
