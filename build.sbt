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
ThisBuild / scalacOptions ++= Seq("-Xmax-inlines:64", "-language:strictEquality")
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
    kernel,
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
  .settings(name := "slurm4s")

lazy val kernel = project
  .in(file("modules/kernel"))
  .settings(commonSettings)
  .settings(
    name := "remote-exec-kernel",
    libraryDependencies ++= Seq(
      Libraries.catsCore,
      Libraries.catsEffect,
      Libraries.scodecBits
    )
  )

lazy val core = project
  .in(file("modules/core"))
  .dependsOn(kernel)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-core",
    libraryDependencies ++= Seq(
      Libraries.catsCore,
      Libraries.circeCore,
      Libraries.circeParser,
      Libraries.scodecBits
    )
  )

lazy val cli = project
  .in(file("modules/cli"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(name := "slurm4s-cli")

lazy val protocol = project
  .in(file("modules/protocol"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-protocol",
    libraryDependencies ++= Seq(
      Libraries.circeCore,
      Libraries.circeGeneric,
      Libraries.circeParser,
      Libraries.scodecBits
    )
  )

lazy val testkit = project
  .in(file("modules/testkit"))
  .dependsOn(core, cli)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-testkit",
    libraryDependencies += Libraries.catsEffect
  )

lazy val local = project
  .in(file("modules/local"))
  .dependsOn(kernel, core, cli, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "slurm4s-local",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    )
  )

lazy val agent = project
  .in(file("modules/agent"))
  .dependsOn(core, protocol, cli, local, worker)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-agent",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    ),
    Compile / mainClass := Some("io.github.bbuchsbaum.slurm4s.agent.AgentMain")
  )

lazy val ssh = project
  .in(file("modules/ssh"))
  .dependsOn(core, protocol, agent, worker, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "slurm4s-ssh",
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
    name := "slurm4s-managed",
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
  .dependsOn(kernel, core, protocol)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-worker",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io
    )
  )

lazy val observability = project
  .in(file("modules/observability"))
  .dependsOn(core, testkit % "test->compile")
  .settings(commonSettings)
  .settings(
    name := "slurm4s-observability",
    libraryDependencies += Libraries.catsEffect
  )

lazy val examples = project
  .in(file("modules/examples"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, cli, protocol, local, agent, ssh, managed, worker, observability)
  .settings(commonSettings)
  .settings(
    name := "slurm4s-examples"
  )

addCommandAlias(
  "checkFormatting",
  ";kernel/scalafmtCheckAll;core/scalafmtCheckAll;cli/scalafmtCheckAll;protocol/scalafmtCheckAll;local/scalafmtCheckAll;agent/scalafmtCheckAll;ssh/scalafmtCheckAll;managed/scalafmtCheckAll;worker/scalafmtCheckAll;observability/scalafmtCheckAll;examples/scalafmtCheckAll;testkit/scalafmtCheckAll;scalafmtSbtCheck"
)
