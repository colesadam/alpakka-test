name := "alpakka-test"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv"   % "2.0.1",
  "org.scalatest" % "scalatest_2.12" % "3.0.4" % "test",
  "com.github.javafaker"        %  "javafaker"                  % "1.0.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.31" % "test"

)
