version := "0.1"
scalaVersion := "2.12.18"
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    s"${name.value}.jar"
}

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.5.0",
    "org.apache.spark" %% "spark-sql" % "3.5.0",
    "org.apache.spark" %% "spark-mllib" % "3.5.0",
    "org.apache.spark" %% "spark-streaming" % "3.5.0",
    "org.apache.spark" %% "spark-hive" % "3.5.0"
    )

//lazy val root = (project in file("."))
//  .settings(
//    name := "Scala"
//  )
