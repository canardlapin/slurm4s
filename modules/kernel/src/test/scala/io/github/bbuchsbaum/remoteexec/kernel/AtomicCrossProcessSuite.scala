package io.github.bbuchsbaum.remoteexec.kernel

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** P8.B3: competing publishers targeting ONE destination, from separate processes.
  *
  * The existing suite races same-JVM fibers, which `AtomicFiles` serializes with a striped monitor
  * before the sidecar lock is ever contended, and its claim race moves one source toward
  * *different* destinations. Neither shape can observe the exists-check/move hole, because the hole
  * only opens when two processes pass the existence check before either renames.
  */
class AtomicCrossProcessSuite extends munit.FunSuite:

  private val contenders = 8

  test("publishOnce admits exactly one publisher across processes") {
    val target = temporaryDirectory().resolve("published.bin")
    val outcomes = race("publishOnce", target)

    assertEquals(
      outcomes.count(_ == "WON"),
      1,
      s"exactly one publisher must win, observed: ${outcomes.mkString(", ")}"
    )
    assert(
      outcomes.filter(_ != "WON").forall(_.startsWith("LOST:TargetExists")),
      s"losers must be classified as TargetExists, observed: ${outcomes.mkString(", ")}"
    )
  }

  test("writeNew admits exactly one writer across processes") {
    val target = temporaryDirectory().resolve("written.bin")
    val outcomes = race("writeNew", target)

    assertEquals(
      outcomes.count(_ == "WON"),
      1,
      s"a second winner means one process silently clobbered another: ${outcomes.mkString(", ")}"
    )
  }

  test("the published bytes belong to exactly one publisher") {
    val target = temporaryDirectory().resolve("content.bin")
    val outcomes = race("writeNew", target)
    val published = new String(Files.readAllBytes(target), "UTF-8")
    val winners = outcomes.count(_ == "WON")

    assertEquals(winners, 1, s"outcomes: ${outcomes.mkString(", ")}")
    assert(
      (0 until contenders).exists(index => published == payload(index)),
      s"published content matches no single contender: '$published'"
    )
  }

  test("writeStable lets identical content succeed and different content conflict") {
    val target = temporaryDirectory().resolve("stable.bin")
    val identical = (0 until contenders).map(_ => "same-bytes").toVector
    val outcomes = spawnAll("writeStable", target, identical, None)

    assertEquals(
      outcomes.count(_ == "WON"),
      contenders,
      s"idempotent publication of identical bytes must succeed for all: ${outcomes.mkString(", ")}"
    )
  }

  test("claim admits exactly one claimant of a contested source") {
    val directory = temporaryDirectory()
    val source = directory.resolve("spool-entry")
    Files.write(source, "work".getBytes("UTF-8"))
    val outcomes = spawnAll(
      "claim",
      source,
      (0 until contenders).map(index => s"claimant-$index").toVector,
      Some(index => directory.resolve(s"claimed-$index"))
    )

    assertEquals(
      outcomes.count(_ == "WON"),
      1,
      s"exactly one claimant may take the source, observed: ${outcomes.mkString(", ")}"
    )
  }

  private def payload(index: Int): String = s"contender-$index"

  private def race(operation: String, target: Path): Vector[String] =
    spawnAll(operation, target, (0 until contenders).map(payload).toVector, None)

  /** Start every contender before reading any output, so they genuinely overlap. */
  private def spawnAll(
      operation: String,
      target: Path,
      payloads: Vector[String],
      destination: Option[Int => Path]
  ): Vector[String] =
    val processes = payloads.zipWithIndex.map { case (value, index) =>
      val command = Vector(
        javaExecutable,
        "-cp",
        System.getProperty("java.class.path"),
        "io.github.bbuchsbaum.remoteexec.kernel.AtomicRaceWorker",
        operation,
        target.toString,
        value
      ) ++ destination.map(_(index).toString).toVector
      new ProcessBuilder(command.asJava).redirectErrorStream(false).start()
    }

    processes.map { process =>
      assert(process.waitFor(60L, TimeUnit.SECONDS), "a contender did not terminate")
      val output = new String(process.getInputStream.readAllBytes(), "UTF-8").trim
      output.linesIterator.toVector.lastOption.getOrElse("LOST:no output")
    }

  private def javaExecutable: String =
    Path.of(System.getProperty("java.home"), "bin", "java").toString

  private def temporaryDirectory(): Path =
    val directory = Files.createTempDirectory("slurm4s-atomic-race")
    directory.toFile.deleteOnExit()
    directory
