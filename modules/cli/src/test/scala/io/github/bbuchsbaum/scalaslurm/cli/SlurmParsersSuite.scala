package io.github.bbuchsbaum.scalaslurm.cli

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

class SlurmParsersSuite extends munit.FunSuite:
  test("sbatch parsable output retains the optional cluster") {
    val parsed = SbatchParsable.parse(evidence("1001;alpha\n"))
    val job = parsed.toOption.get

    assertEquals(job.jobId.value, "1001")
    assertEquals(job.cluster.map(_.value), Some("alpha"))
  }

  test("sbatch parsable output rejects extra lines or delimiters") {
    assert(SbatchParsable.parse(evidence("1001;alpha;extra\n")).isLeft)
    assert(SbatchParsable.parse(evidence("1001\nwarning\n")).isLeft)
  }

  test("version-scoped squeue JSON retains unknown states and raw fields") {
    val raw = resource("/fixtures/slurm-v0.0.43/squeue-pending.json")
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None, None))
    val observations = SqueueJsonV0043.parse(evidence(raw), expected).toOption.get

    assertEquals(observations.length, 2)
    assertEquals(observations.head.state, SlurmState.Pending)
    assertEquals(observations(1).state, SlurmState.Unknown("RESIZING_FUTURE"))
    assert(observations.head.rawFields.contains("state_reason"))
  }

  test("strict accounting fallback distinguishes OOM from ordinary non-zero exit") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None, None))
    val oom = SacctParsable2.parse(evidence("1001|OUT_OF_MEMORY|0:9|OutOfMemory\n"), expected)
    val failed = SacctParsable2.parse(evidence("1001|FAILED|2:0|NonZeroExitCode\n"), expected)

    assertEquals(oom.toOption.get.head.outcome, Some(WorkloadOutcome.OutOfMemory))
    assert(failed.toOption.get.head.outcome.exists(_.isInstanceOf[WorkloadOutcome.Failed]))
    assert(SacctParsable2.parse(evidence("1001|FAILED|bad|reason\n"), expected).isLeft)
  }

  test("nonterminal accounting rows carry no terminal workload outcome") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None, None))
    val running = SacctParsable2.parse(evidence("1001|RUNNING|0:0|None\n"), expected)

    assertEquals(running.toOption.get.head.outcome, None)
  }

  test("array observations and partial failures never alias sibling elements") {
    val parent = JobId.from("9100").toOption.get
    val first = JobRef(parent, None, Some(ArrayIndex.from(0).toOption.get))
    val second = JobRef(parent, None, Some(ArrayIndex.from(1).toOption.get))
    val expected = NonEmptyVector.of(first, second)
    val queueJson =
      """{"jobs":[{"job_id":9101,"array_job_id":{"set":true,"infinite":false,"number":9100},"array_task_id":{"set":true,"infinite":false,"number":0},"job_state":["COMPLETED"]},{"job_id":9102,"array_job_id":{"set":true,"infinite":false,"number":9100},"array_task_id":{"set":true,"infinite":false,"number":1},"job_state":["FAILED"]}]}"""

    val queue = SqueueJsonV0043.parse(evidence(queueJson), expected).toOption.get
    val accounting = SacctParsable2
      .parse(
        evidence("9100_0|COMPLETED|0:0|None\n9100_1|OUT_OF_MEMORY|0:9|OutOfMemory\n"),
        expected
      )
      .toOption
      .get

    assertEquals(queue.map(_.job), Vector(first, second))
    assertEquals(accounting.map(_.job), Vector(first, second))
    assertEquals(accounting.head.outcome, Some(WorkloadOutcome.Completed(0)))
    assertEquals(accounting(1).outcome, Some(WorkloadOutcome.OutOfMemory))
  }

  test("v0.0.43 fixtures are routed explicitly across 25.05 and 26.05 shapes") {
    val families = Vector(
      ("25.05", "/fixtures/slurm-25.05-v0.0.43/stdout.json", "2505001"),
      ("26.05", "/fixtures/slurm-26.05-v0.0.43/stdout.json", "2605001")
    )
    val parser = DataParserVersion.from("v0.0.43").toOption.get

    families.foreach { case (family, path, jobText) =>
      val job = JobId.from(jobText).toOption.get
      val expected = NonEmptyVector.of(
        JobRef(job, None, Some(ArrayIndex.from(0).toOption.get)),
        JobRef(job, None, Some(ArrayIndex.from(1).toOption.get))
      )
      val parsed = VersionedSqueueParsers.parse(parser, evidence(resource(path)), expected)
      assert(parsed.isRight, clues(family, parsed))
      assertEquals(parsed.toOption.get.map(_.job.arrayIndex), expected.toVector.map(_.arrayIndex))
    }
  }

  test("actual Slurm 25.05.6 compressed array output is parsed without unbounded expansion") {
    val parent = JobId.from("1").toOption.get
    val expected = NonEmptyVector.of(
      JobRef(parent, None, Some(ArrayIndex.from(0).toOption.get)),
      JobRef(parent, None, Some(ArrayIndex.from(1).toOption.get))
    )
    val directory = "/fixtures/slurm-25.05.6-v0.0.43-actual"
    val parsed = VersionedSqueueParsers.parse(
      DataParserVersion.from("v0.0.43").toOption.get,
      evidence(resource(s"$directory/stdout.json")),
      expected
    )

    assert(parsed.isRight, parsed)
    val cluster = ClusterName.from("cluster").toOption.get
    assertEquals(
      parsed.toOption.get.map(_.job),
      expected.toVector.map(_.copy(cluster = Some(cluster)))
    )
    assert(parsed.toOption.get.forall(_.state == SlurmState.Pending))

    val provenance =
      io.circe.parser.parse(resource(s"$directory/provenance.json")).toOption.get.hcursor
    Vector("stdout" -> "stdout.json", "stderr" -> "stderr.txt").foreach { case (stream, file) =>
      val bytes = resourceBytes(s"$directory/$file")
      assertEquals(
        provenance.downField("streams").downField(stream).get[Long]("bytes").toOption.get,
        bytes.length.toLong
      )
      assertEquals(
        provenance.downField("streams").downField(stream).get[String]("sha256").toOption.get,
        sha256(bytes)
      )
    }
    assertEquals(provenance.get[Boolean]("parserCompatibilityClaim").toOption, Some(true))
    assertEquals(provenance.get[Boolean]("siteSupportClaim").toOption, Some(false))
    assertEquals(provenance.get[Boolean]("supportClaim").toOption, Some(false))
  }

  test("unset no-value array fields preserve an ordinary job identity") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("9200").toOption.get, None, None))
    val raw =
      """{"jobs":[{"job_id":9200,"array_job_id":{"set":false,"infinite":false,"number":0},"array_task_id":{"set":false,"infinite":false,"number":0},"job_state":["RUNNING"]}]}"""

    val parsed = SqueueJsonV0043.parse(evidence(raw), expected).toOption.get

    assertEquals(parsed.map(_.job), Vector(expected.head))
  }

  test("a compressed pending array record expands only requested matching elements") {
    val parent = JobId.from("9300").toOption.get
    val expected = NonEmptyVector.of(
      JobRef(parent, None, Some(ArrayIndex.from(1).toOption.get)),
      JobRef(parent, None, Some(ArrayIndex.from(2).toOption.get)),
      JobRef(parent, None, Some(ArrayIndex.from(7).toOption.get))
    )
    val raw =
      """{"jobs":[{"job_id":9300,"array_job_id":{"set":true,"infinite":false,"number":9300},"array_task_id":{"set":false,"infinite":false,"number":0},"array_task_string":"1-5:2,7%2","job_state":["PENDING"],"state_reason":"Priority"}]}"""

    val parsed = SqueueJsonV0043.parse(evidence(raw), expected).toOption.get

    assertEquals(
      parsed.map(_.job.arrayIndex.flatMap(index => Some(index.value))),
      Vector(Some(1), Some(7))
    )
    assert(parsed.forall(_.state == SlurmState.Pending))
  }

  test("version-scoped fixture provenance matches exact raw stream bytes") {
    Vector("25.05", "26.05").foreach { family =>
      val directory = s"/fixtures/slurm-$family-v0.0.43"
      val provenance = io.circe.parser.parse(resource(s"$directory/provenance.json")).toOption.get
      val cursor = provenance.hcursor

      Vector("stdout" -> "stdout.json", "stderr" -> "stderr.txt").foreach { case (stream, file) =>
        val bytes = resourceBytes(s"$directory/$file")
        assertEquals(
          cursor.downField("streams").downField(stream).get[Long]("bytes").toOption.get,
          bytes.length.toLong
        )
        assertEquals(
          cursor.downField("streams").downField(stream).get[String]("sha256").toOption.get,
          sha256(bytes)
        )
      }
      assertEquals(cursor.get[Boolean]("supportClaim").toOption, Some(false))
    }
  }

  private def evidence(value: String): BoundedEvidence =
    BoundedEvidence.capture(
      EvidenceSource.CommandStdout("fixture"),
      Instant.parse("2026-07-22T12:00:00Z"),
      value.getBytes(StandardCharsets.UTF_8).toVector
    )

  private def resource(path: String): String =
    String(resourceBytes(path), StandardCharsets.UTF_8)

  private def resourceBytes(path: String): Array[Byte] =
    val stream = Option(getClass.getResourceAsStream(path)).get
    try stream.readAllBytes()
    finally stream.close()

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
