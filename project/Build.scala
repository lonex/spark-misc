import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

// dependencies
object Dependencies {
  val resolutionRepos = Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "twitter-repo" at "http://maven.twttr.com",
    "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local"
  )

  object V {
    val spark  = "1.2.1"
    val specs2    = "2.4.15"
    val cascading = "2.5.2"
    val joda = "2.6"
    val jackson= "2.3.3"
    val awscala = "0.4.3"
    val akka = "2.3.4"
    val phantom = "1.5.0"
    val pillar = "2.0.1"
  }

  object Libraries {
    val sparkCore = "org.apache.spark" % "spark-core_2.10" % V.spark % "provided"
    val joda = "joda-time" % "joda-time" % V.joda
    val jodaConvert = "org.joda" % "joda-convert" % "1.7"
    val jackson= "com.lambdaworks" % "jacks_2.10" % V.jackson
    val awscala = "com.github.seratch" %% "awscala" % V.awscala
    val scopt = "com.github.scopt" %% "scopt" % "3.3.0"
    val akka = "com.typesafe.akka" %% "akka-actor" % V.akka
    val phantomDsl = "com.websudos"  %% "phantom-dsl" % V.phantom
    val pillar = "com.chrisomeara" %% "pillar" % V.pillar

    // Scala (test only)
    val specs2       = "org.specs2" %% "specs2-core" % V.specs2 % "test"
  }
}
 
// build settings
object BuildSettings {

  val projectVersion = "0.0.1"
    
  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.acme",
    version       := projectVersion,
    description   := "The Spark Misc project",
    scalaVersion  := "2.10.4",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    resolvers     ++= Dependencies.resolutionRepos,
    scalacOptions in Test ++= Seq("-Yrangepos")
  )
 
  lazy val sbtAssemblySettings = Seq(
    (assemblyJarName in assembly) := s"extract_${projectVersion}.jar",
    // Skip test in assembly
    (test in assembly) := {}, 

    (assemblyMergeStrategy in assembly) := {
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
      case PathList("application.conf") => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}

object SparkExtractBuild extends Build {
  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  lazy val root = Project("spark-misc", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.joda,
        Libraries.jodaConvert,
        Libraries.jackson,
        Libraries.sparkCore,
        Libraries.awscala,
        Libraries.scopt,
        Libraries.akka,
        Libraries.phantomDsl,
        Libraries.pillar,
        Libraries.specs2
        // Add your additional libraries here (comma-separated)...
      )
    )
}
