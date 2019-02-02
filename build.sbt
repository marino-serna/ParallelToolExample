import sbt.Keys.organization

lazy val root = (project in file("."))
  .settings(
    name := "parallel-tool-example",
    version := "1.0.0-00",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("com.github.marino_serna.parallel_tool_example.ParallelToolExampleMain"),

    organization := "com.github.marino-serna",
    homepage := Some(url("https://github.com/marino-serna/ParallelToolExample")),
    scmInfo := Some(ScmInfo(url("https://github.com/marino-serna/ParallelToolExample"),
      "git@github.com:marino-serna/ParallelToolExample.git")),
    developers := List(Developer("marino-serna",
      "Marino Serna",
      "marinosersan@gmail.com",
      url("https://github.com/marino-serna"))),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
  )

val sparkVersion = "2.2.1"

// disable using the Scala version in output paths and artifacts
crossPaths := false
useGpg := true
publishMavenStyle := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.marino-serna" % "parallel-tool" % "1.0.1-01",
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentialsPublic")

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)