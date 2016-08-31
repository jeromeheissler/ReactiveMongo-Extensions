name := "reactivemongo-extensions-core"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % Common.reactiveMongoVersion,
  "com.typesafe.play" %% "play-json" % Common.playVersion % "provided",
  "com.typesafe" % "config" % "1.3.0",
  "joda-time" % "joda-time" % "2.9.4",
  "org.joda" % "joda-convert" % "1.8.1",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "ch.qos.logback" % "logback-classic" % "1.1.7" % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test")
