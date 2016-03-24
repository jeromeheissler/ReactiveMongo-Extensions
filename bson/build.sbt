import Common.{ playVersion, playReactiveMongoVersion }

name := "reactivemongo-extensions-bson"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "play2-reactivemongo" % playReactiveMongoVersion,
  "org.reactivemongo" %% "reactivemongo-iteratees" % playReactiveMongoVersion,
  "com.typesafe.play" %% "play-json" % playVersion % "provided"
)
