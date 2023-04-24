name := "reactive-systems"
scalaVersion := "3.1.0"

lazy val commons = Seq(
  course := "reactive",
  scalaVersion := "3.1.0",

  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-encoding", "UTF-8",
    "-unchecked"
  ),

  libraryDependencies += "org.scalameta" %% "munit" % "0.7.26" % Test,
  Test / parallelExecution := false,
)

lazy val root = (project in file("."))
  .aggregate(async, actorbintree)

lazy val async = (project in file("week1-async"))
  .enablePlugins(StudentTasks)
  .settings(commons: _*)

lazy val actorbintree = (project in file("week2-actorbintree"))
  .enablePlugins(StudentTasks)
  .settings(commons: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.6.18",
      "com.typesafe.akka" %% "akka-testkit" % "2.6.18" % Test,
    )
  )
