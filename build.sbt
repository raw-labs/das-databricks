import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd, LayeredMapping}
import sbt.Keys._
import sbt._

import java.nio.file.Paths

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "raw-labs",
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

lazy val commonSettings = Seq(
  homepage := Some(url("https://www.raw-labs.com/")),
  organization := "com.raw-labs",
  organizationName := "RAW Labs SA",
  organizationHomepage := Some(url("https://www.raw-labs.com/")),
  // Use cached resolution of dependencies
  // http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.in(Global).value.withCachedResolution(true),
  resolvers ++= Seq(Resolver.mavenLocal),
  resolvers += "RAW Labs GitHub Packages" at "https://maven.pkg.github.com/raw-labs/_"
)

lazy val buildSettings = Seq(
  scalaVersion := "2.13.16",
  javacOptions ++= Seq(
    "-source",
    "21",
    "-target",
    "21"
  ),
  scalacOptions ++= Seq(
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint:-stars-align,_",
    "-Ywarn-dead-code",
    "-Ywarn-macros:after", // Fix for false warning of unused implicit arguments in traits/interfaces.
    "-Ypatmat-exhaust-depth",
    "160"
  )
)

lazy val compileSettings = Seq(
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / mappings := Seq(),
  Compile / packageSrc / publishArtifact := true,
  Compile / packageDoc / publishArtifact := false,
  Compile / packageBin / packageOptions += Package.ManifestAttributes(
    "Automatic-Module-Name" -> name.value.replace('-', '.')
  ),
  // Ensure Java annotations get compiled first, so that they are accessible from Scala.
  compileOrder := CompileOrder.JavaThenScala
)

lazy val testSettings = Seq(
  // Ensuring tests are run in a forked JVM for isolation.
  Test / fork := true,
  // Disabling parallel execution of tests.
  //Test / parallelExecution := false,
  // Pass system properties starting with "raw." to the forked JVMs.
  Test / javaOptions ++= {
    import scala.collection.JavaConverters._
    val props = System.getProperties
    props
      .stringPropertyNames()
      .asScala
      .filter(_.startsWith("raw."))
      .map(key => s"-D$key=${props.getProperty(key)}")
      .toSeq
  },
  // Set up heap dump options for out-of-memory errors.
  Test / javaOptions ++= Seq(
    "-XX:+HeapDumpOnOutOfMemoryError",
    s"-XX:HeapDumpPath=${Paths.get(sys.env.getOrElse("SBT_FORK_OUTPUT_DIR", "target/test-results")).resolve("heap-dumps")}"
  ),
  Test / publishArtifact := true
)

val isCI = sys.env.getOrElse("CI", "false").toBoolean

lazy val publishSettings = Seq(
  versionScheme := Some("early-semver"),
  publish / skip := false,
  publishMavenStyle := true,
  publishTo := Some("GitHub raw-labs Apache Maven Packages" at "https://maven.pkg.github.com/raw-labs/das-databricks"),
  publishConfiguration := publishConfiguration.value.withOverwrite(isCI)
)

lazy val strictBuildSettings = commonSettings ++ compileSettings ++ buildSettings ++ testSettings ++ Seq(
  scalacOptions ++= Seq(
    "-Xfatal-warnings"
  )
)

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "das-databricks",
    strictBuildSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "com.raw-labs" %% "protocol-das" % "1.0.0" % "compile->compile;test->test",
      "com.raw-labs" %% "das-server-scala" % "0.3.0" % "compile->compile;test->test",
      "com.databricks" % "databricks-sdk-java" % "0.31.1" % "compile->compile"
    ),
    // Apply Docker settings
    dockerSettings,
    Compile / mainClass := Some("com.rawlabs.das.server.DASServer")
  )

lazy val dockerSettings = Seq(
  Docker / packageName := "das-databricks-server",

  dockerBaseImage := "eclipse-temurin:21-jre",

  dockerLabels ++= Map(
    "vendor" -> "RAW Labs SA",
    "product" -> "das-databricks-server",
    "image-type" -> "final",
    "org.opencontainers.image.source" -> "https://github.com/raw-labs/das-databricks"
  ),

  Docker / daemonUser := "raw",
  Docker / daemonUserUid := Some("1001"),
  Docker / daemonGroup := "raw",
  Docker / daemonGroupGid := Some("1001"),

  dockerExposedVolumes := Seq("/var/log/raw"),
  dockerExposedPorts := Seq(50051),

  dockerEnvVars ++= Map(
    "LANG" -> "C.UTF-8"
  ),

  dockerCommands := {
    val setupCommands = Seq(
      Cmd("RUN", Seq(
        "apt-get update",
        "apt-get install -y --no-install-recommends netcat-openbsd",
        "apt-get clean",
        "rm -rf /var/lib/apt/lists/*",
        "mkdir -p /var/log/raw"
      ).mkString(" && \\\n"))
    )

    // Add healthcheck command (will be after USER command)
    val healthcheckCommand = Seq(
      Cmd("HEALTHCHECK", "--interval=30s", "--timeout=10s", "--start-period=60s", "--retries=3",
          "CMD", "[", "\"/opt/docker/bin/healthcheck.sh\"", "]")
    )

    // Get the default commands from sbt-native-packager
    val defaultCommands = dockerCommands.value

    // Find the position after FROM for the main stage
    val fromCommandPos = defaultCommands.indexWhere {
      case Cmd("FROM", args @ _*) if args.headOption.contains("eclipse-temurin:21-jre") && !args.contains("stage0") => true
      case _ => false
    }

    // Insert our setup commands after the FROM command and add healthcheck at the end
    val (before, after) = defaultCommands.splitAt(fromCommandPos + 1)
    before ++ setupCommands ++ after ++ healthcheckCommand
  },

  // Include healthcheck script
  Universal / mappings ++= Seq(
    file("src/main/resources/healthcheck.sh") -> "bin/healthcheck.sh"
  ),

  // Layer configuration
  Docker / dockerLayerMappings := (Docker / dockerLayerMappings).value.map {
    case lm @ LayeredMapping(Some(1), file, path) => {
      val fileName = java.nio.file.Paths.get(path).getFileName.toString
      if (!fileName.endsWith(".jar")) {
        // Config files in top layer
        LayeredMapping(Some(2), file, path)
      } else if (fileName.startsWith("com.raw-labs") && fileName.endsWith(".jar")) {
        // Our jars in top layer
        LayeredMapping(Some(2), file, path)
      } else {
        // 3rd party libs in base layer
        lm
      }
    }
    case lm @ _ => lm
  },

  dockerAlias := dockerAlias.value.withTag(Option(version.value.replace("+", "-"))),
  dockerAliases := {
    val devRegistry = sys.env.getOrElse("DEV_REGISTRY", "ghcr.io/raw-labs/das-databricks")
    val releaseRegistry = sys.env.get("RELEASE_DOCKER_REGISTRY")
    val baseAlias = dockerAlias.value.withRegistryHost(Some(devRegistry))

    releaseRegistry match {
      case Some(releaseReg) => Seq(
          baseAlias,
          dockerAlias.value.withRegistryHost(Some(releaseReg))
        )
      case None => Seq(baseAlias)
    }
  }
)

lazy val printDockerImageName = taskKey[Unit]("Prints the full Docker image name that will be produced")

printDockerImageName := {
  // Get the main Docker alias (the first one in the sequence)
  val alias = (Docker / dockerAliases).value.head
  // The toString method already returns the full image name with registry and tag
  println(s"DOCKER_IMAGE=${alias}")
}
