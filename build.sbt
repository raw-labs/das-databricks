import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-databricks",
    libraryDependencies ++= Seq(
      // DAS
      "com.raw-labs" %% "das-server-scala" % "0.7.1-dep1" % "compile->compile;test->test",
      // Databricks
      "com.databricks" % "databricks-sdk-java" % "0.41.0" % "compile->compile"),
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-handler" % "4.1.118.Final"
    ))
