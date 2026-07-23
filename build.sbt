import Dependencies.*

ThisBuild / organization := "io.github.bbuchsbaum"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Versions.scala3
ThisBuild / crossScalaVersions := Seq(Versions.scala3)
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / javacOptions ++= Seq("--release", "17")
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-release:17",
  "-unchecked",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Xmax-inlines:64"
)
ThisBuild / Test / fork := true

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    Libraries.munit % Test,
    Libraries.munitCatsEffect % Test,
    Libraries.munitScalaCheck % Test,
    Libraries.scalaCheck % Test
  ),
  Test / parallelExecution := true
)

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    cli,
    protocol,
    testkit,
    local,
    agent,
    ssh,
    managed,
    worker,
    observability,
    examples
  )
  .settings(
    name := "scala-slurm",
    publish / skip := true
  )

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-core",
    libraryDependencies ++= Seq(
      Libraries.catsCore,
      Libraries.circeCore,
      Libraries.circeParser
    )
  )

lazy val cli = project
  .in(file("modules/cli"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "scala-slurm-cli")

lazy val protocol = project
  .in(file("modules/protocol"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-protocol",
    libraryDependencies ++= Seq(
      Libraries.circeCore,
      Libraries.circeGeneric,
      Libraries.circeParser
    )
  )

lazy val testkit = project
  .in(file("modules/testkit"))
  .dependsOn(core, cli)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-testkit",
    libraryDependencies += Libraries.catsEffect
  )

lazy val local = project
  .in(file("modules/local"))
  .dependsOn(core, cli, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-local",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    )
  )

lazy val agent = project
  .in(file("modules/agent"))
  .dependsOn(core, protocol, cli, local)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-agent",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    ),
    Compile / mainClass := Some("io.github.bbuchsbaum.scalaslurm.agent.AgentMain")
  )

lazy val ssh = project
  .in(file("modules/ssh"))
  .dependsOn(core, protocol, agent, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-ssh",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    )
  )

lazy val managed = project
  .in(file("modules/managed"))
  .dependsOn(core, protocol, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-managed",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.circeCore,
      Libraries.circeParser
    )
  )

lazy val worker = project
  .in(file("modules/worker"))
  .dependsOn(core, protocol)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-worker",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    )
  )

lazy val observability = project
  .in(file("modules/observability"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-observability",
    libraryDependencies += Libraries.catsEffect
  )

lazy val examples = project
  .in(file("modules/examples"))
  .dependsOn(core, cli, protocol, local, agent, ssh, managed, worker)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-examples",
    publish / skip := true
  )

addCommandAlias(
  "checkFormatting",
  ";core/scalafmtCheckAll;cli/scalafmtCheckAll;protocol/scalafmtCheckAll;local/scalafmtCheckAll;agent/scalafmtCheckAll;ssh/scalafmtCheckAll;managed/scalafmtCheckAll;worker/scalafmtCheckAll;observability/scalafmtCheckAll;examples/scalafmtCheckAll;testkit/scalafmtCheckAll;scalafmtSbtCheck"
)
