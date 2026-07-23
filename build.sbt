import Dependencies.*

ThisBuild / tlBaseVersion := "0.1"
ThisBuild / organization := "io.github.bbuchsbaum"
ThisBuild / organizationName := "Bradley Buchsbaum"
ThisBuild / startYear := Some(2026)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(tlGitHubDev("bbuchsbaum", "Bradley Buchsbaum"))
ThisBuild / scalaVersion := Versions.scala3
ThisBuild / crossScalaVersions := Seq(Versions.scala3)
ThisBuild / tlJdkRelease := Some(17)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"), JavaSpec.temurin("21"))
ThisBuild / tlCiScalafmtCheck := true
ThisBuild / tlCiHeaderCheck := false
ThisBuild / scalacOptions += "-Xmax-inlines:64"
ThisBuild / Test / fork := true

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    Libraries.munit % Test,
    Libraries.munitCatsEffect % Test,
    Libraries.munitScalaCheck % Test,
    Libraries.scalaCheck % Test
  )
)

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
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
    site,
    examples
  )
  .settings(name := "scala-slurm")

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

// Site layer: scheduler-neutral compute/data facade over the Slurm modules.
// Depends only on core until the in-flight cli/protocol work lands; will widen
// to managed/worker/ssh/local for the integration milestones (see docs/plans/).
lazy val site = project
  .in(file("modules/site"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-site",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.circeCore,
      Libraries.circeParser
    )
  )

lazy val examples = project
  .in(file("modules/examples"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, cli, protocol, local, agent, ssh, managed, worker, observability)
  .settings(commonSettings)
  .settings(
    name := "scala-slurm-examples"
  )

addCommandAlias(
  "checkFormatting",
  ";core/scalafmtCheckAll;cli/scalafmtCheckAll;protocol/scalafmtCheckAll;local/scalafmtCheckAll;agent/scalafmtCheckAll;ssh/scalafmtCheckAll;managed/scalafmtCheckAll;worker/scalafmtCheckAll;observability/scalafmtCheckAll;site/scalafmtCheckAll;examples/scalafmtCheckAll;testkit/scalafmtCheckAll;scalafmtSbtCheck"
)
