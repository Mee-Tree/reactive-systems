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
  .aggregate(async)

lazy val async = (project in file("week1-async"))
  .enablePlugins(StudentTasks)
  .settings(commons: _*)
