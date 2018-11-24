name := "minie-spark"

version := "0.1"

scalaVersion := "2.11.12"

//retrieveManaged := true

resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
// sys.props += "packaging.type" -> "jar"

// assembly
mainClass in assembly := Some("Main")
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
// case x => MergeStrategy.first
 case n if n.startsWith("reference.conf") => MergeStrategy.concat
 case _ => MergeStrategy.first
}

// etc
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + SPARK_VERSION + "_" + module.revision + "." + artifact.extension
}


val JACKSON_VERSION = "2.8.8"
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % JACKSON_VERSION,
  "com.fasterxml.jackson.core" % "jackson-databind" % JACKSON_VERSION,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % JACKSON_VERSION
)

val SPARK_VERSION = "2.3.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "de.uni_mannheim" % "minie" % "0.0.1-SNAPSHOT"
)