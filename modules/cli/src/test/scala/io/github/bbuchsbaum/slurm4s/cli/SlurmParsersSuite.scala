package io.github.bbuchsbaum.slurm4s.cli

import cats.data.NonEmptyVector
import io.github.bbuchsbaum.slurm4s.core.*

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.time.LocalDateTime

class SlurmParsersSuite extends munit.FunSuite:
  test("a federated sbatch response is refused rather than silently narrowed") {
    // `sbatch --parsable` appends `;cluster` only on a federated submission. Accepting the id and
    // discarding the cluster would yield a JobRef that addresses the wrong cluster on every later
    // query and cancellation, so v0.1 refuses the site it cannot address.
    val parsed = SbatchParsable.parse(evidence("1001;alpha\n"))

    assert(parsed.isLeft, s"expected a federation refusal, observed $parsed")
    assertEquals(
      parsed.left.toOption.get.values.head.code,
      "federation-unsupported"
    )
  }

  test("an ordinary sbatch response parses to a single-cluster identity") {
    val job = SbatchParsable.parse(evidence("1001\n")).toOption.get

    assertEquals(job.jobId.value, "1001")
    assertEquals(job.arrayIndex, None)
  }

  test("sbatch parsable output rejects extra lines or delimiters") {
    assert(SbatchParsable.parse(evidence("1001;alpha;extra\n")).isLeft)
    assert(SbatchParsable.parse(evidence("1001\nwarning\n")).isLeft)
  }

  test("version-scoped squeue JSON retains unknown states and raw fields") {
    val raw = resource("/fixtures/slurm-v0.0.43/squeue-pending.json")
    val expected = NonEmptyVector.of(
      JobRef(JobId.from("1001").toOption.get, None),
      JobRef(JobId.from("1002").toOption.get, None)
    )
    val observations = SqueueJsonV0043.parse(evidence(raw), expected).toOption.get

    assertEquals(observations.length, 2)
    assertEquals(observations.head.state, SlurmState.Pending)
    assertEquals(observations(1).state, SlurmState.Unknown("RESIZING_FUTURE"))
    assert(observations.head.rawFields.contains("state_reason"))
  }

  test("edge fixtures preserve unknown data and isolate an unrelated malformed row") {
    val directory = "/fixtures/slurm-parser-edge-v1"
    val focused =
      ScontrolOneliner.parse(evidence(resource(s"$directory/scontrol.txt"))).toOption.get
    val expected = NonEmptyVector.one(JobRef(JobId.from("9300").toOption.get, None))
    val queue =
      SqueueJsonV0043.parse(evidence(resource(s"$directory/squeue.json")), expected).toOption.get

    assertEquals(focused("Command"), "/opt/run modèle --label étude")
    assertEquals(focused("UnknownSiteField"), "α|β")
    assertEquals(focused("EmptyField"), "")
    assertEquals(focused("Reason"), "Waiting for café nodes")
    assertEquals(queue.size, 1)
    assertEquals(queue.head.job, expected.head)
    assertEquals(queue.head.state, SlurmState.Unknown("FUTURE_STATE"))
    assertEquals(queue.head.reason, Some("Waiting for café nodes"))
    assertEquals(queue.head.rawFields("comment"), "\"\"")
    assertEquals(queue.head.rawFields("site_metadata"), """{"label":"α|β"}""")
    assertEquals(
      queue.head.evidence.primary.bytes,
      ByteVector.view(resource(s"$directory/squeue.json").getBytes(StandardCharsets.UTF_8))
    )
  }

  test("parser edge fixture provenance matches exact raw stream bytes") {
    val directory = "/fixtures/slurm-parser-edge-v1"
    val provenance = io.circe.parser.parse(resource(s"$directory/provenance.json")).toOption.get
    val cursor = provenance.hcursor

    Vector("scontrol" -> "scontrol.txt", "squeue" -> "squeue.json").foreach { case (stream, file) =>
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
    assertEquals(cursor.get[Boolean]("clusterCapture").toOption, Some(false))
  }

  test("structured queue timing distinguishes actual and expected starts") {
    val running = JobRef(JobId.from("2001").toOption.get, None)
    val pending = JobRef(JobId.from("2002").toOption.get, None)
    val raw =
      """{"jobs":[
        |{"job_id":2001,"job_state":["RUNNING"],"start_time":{"set":true,"infinite":false,"number":1784800800},"end_time":{"set":true,"infinite":false,"number":1784806200},"time_limit":{"set":true,"infinite":false,"number":90}},
        |{"job_id":2002,"job_state":["PENDING"],"start_time":{"set":true,"infinite":false,"number":1784804400},"end_time":{"set":true,"infinite":false,"number":0},"time_limit":{"set":false,"infinite":true,"number":0}}
        |]}""".stripMargin

    val observations =
      SqueueJsonV0043.parse(evidence(raw), NonEmptyVector.of(running, pending)).toOption.get

    assertEquals(
      observations.head.timing,
      JobTiming(
        Some(
          JobStart.Actual(
            SchedulerTimestamp.Absolute(Instant.ofEpochSecond(1784800800L))
          )
        ),
        Some(SchedulerTimestamp.Absolute(Instant.ofEpochSecond(1784806200L))),
        ObservedTimeLimit.Limited(WallTimeMinutes.from(90).toOption.get)
      )
    )
    assertEquals(
      observations(1).timing.start,
      Some(
        JobStart.Expected(
          SchedulerTimestamp.Absolute(Instant.ofEpochSecond(1784804400L))
        )
      )
    )
    assertEquals(observations(1).timing.projectedEndAt, None)
    assertEquals(observations(1).timing.timeLimit, ObservedTimeLimit.Unlimited)
  }

  test("malformed optional queue timing degrades to unknown without losing the observation") {
    val job = JobRef(JobId.from("2003").toOption.get, None)
    val raw =
      """{"jobs":[{"job_id":2003,"job_state":["RUNNING"],"start_time":"not-a-time","end_time":{"unexpected":true},"time_limit":{"set":true,"infinite":false}}]}"""

    val observation =
      SqueueJsonV0043.parse(evidence(raw), NonEmptyVector.one(job)).toOption.get.head

    assertEquals(observation.state, SlurmState.Running)
    assertEquals(observation.timing.start, None)
    assertEquals(observation.timing.projectedEndAt, None)
    assert(observation.timing.timeLimit.isInstanceOf[ObservedTimeLimit.Unknown])
    assert(observation.rawFields.keySet.contains("time_limit"))
  }

  test("scontrol parsing retains spaced values and site-local timing facts") {
    val parsed = ScontrolOneliner
      .parse(
        evidence(
          "JobId=2004 JobState=RUNNING Command=/opt/run model --name example StartTime=2026-07-23T10:00:00 EndTime=2026-07-23T11:30:00 TimeLimit=01:30:00 Reason=Node failure detected\n"
        )
      )
      .toOption
      .get
    val timing = ScontrolOneliner.timing(parsed)

    assertEquals(parsed.get("Command"), Some("/opt/run model --name example"))
    assertEquals(parsed.get("Reason"), Some("Node failure detected"))
    assertEquals(
      timing.start,
      Some(
        JobStart.Actual(
          SchedulerTimestamp.SiteLocal(LocalDateTime.parse("2026-07-23T10:00:00"))
        )
      )
    )
    assertEquals(
      timing.projectedEndAt,
      Some(SchedulerTimestamp.SiteLocal(LocalDateTime.parse("2026-07-23T11:30:00")))
    )
    assertEquals(
      timing.timeLimit,
      ObservedTimeLimit.Limited(WallTimeMinutes.from(90).toOption.get)
    )
  }

  test("scontrol timing preserves unlimited and partition-derived limits") {
    assertEquals(
      ScontrolOneliner.timing(Map("JobState" -> "PENDING", "TimeLimit" -> "UNLIMITED")).timeLimit,
      ObservedTimeLimit.Unlimited
    )
    assertEquals(
      ScontrolOneliner
        .timing(Map("JobState" -> "PENDING", "TimeLimit" -> "Partition_Limit"))
        .timeLimit,
      ObservedTimeLimit.PartitionDefault
    )
  }

  test("every documented Slurm duration shape parses to a limit") {
    // `squeue` renders a sub-hour limit as MM:SS, so a 30-minute job reports "30:00". The
    // day-hour forms are the remaining documented shapes. Each previously fell through to
    // Unknown, which an operator reads as "the site did not report a limit".
    val cases = Vector(
      "30:00" -> 30L, // MM:SS
      "5:00" -> 5L, // MM:SS, single-digit minutes
      "0:30" -> 1L, // MM:SS, rounded up as elsewhere
      "2-12" -> 3600L, // D-HH
      "2-12:30" -> 3630L, // D-HH:MM
      "1-00:00:00" -> 1440L, // D-HH:MM:SS, already supported
      "01:30:00" -> 90L, // HH:MM:SS, already supported
      "45" -> 45L // bare minutes, already supported
    )

    cases.foreach { case (text, expected) =>
      assertEquals(
        ScontrolOneliner.timing(Map("JobState" -> "RUNNING", "TimeLimit" -> text)).timeLimit,
        ObservedTimeLimit.Limited(WallTimeMinutes.from(expected).toOption.get),
        s"TimeLimit=$text should parse to $expected minutes"
      )
    }
  }

  test("strict accounting fallback distinguishes OOM from ordinary non-zero exit") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None))
    val oom = SacctParsable2.parse(evidence("1001|OUT_OF_MEMORY|0:9|OutOfMemory\n"), expected)
    val failed = SacctParsable2.parse(evidence("1001|FAILED|2:0|NonZeroExitCode\n"), expected)

    assertEquals(oom.toOption.get.head.outcome, Some(WorkloadOutcome.OutOfMemory))
    assert(failed.toOption.get.head.outcome.exists(_.isInstanceOf[WorkloadOutcome.Failed]))

    // An unreadable exit code for a requested job is a parse failure. Returning no records would
    // report the job as absent from accounting, which is a different and untrue statement.
    val unreadable = SacctParsable2.parse(evidence("1001|FAILED|bad|reason\n"), expected)
    assertEquals(
      unreadable.left.toOption.map(_.toVector.map(_.code)),
      Some(Vector("invalid-accounting-exit-code"))
    )
  }

  test("nonterminal accounting rows carry no terminal workload outcome") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None))
    val running = SacctParsable2.parse(evidence("1001|RUNNING|0:0|None\n"), expected)

    assertEquals(running.toOption.get.head.outcome, None)
  }

  test("requeue states are typed consistently and remain nonterminal in accounting") {
    val expected = NonEmptyVector.one(JobRef(JobId.from("1001").toOption.get, None))
    // SchedMD documents these as state FLAGS, so each yields a flag with no known base state
    // rather than a fabricated one. They must still be nonterminal.
    val states = Vector(
      "REQUEUED" -> SlurmStateFlag.Requeued,
      "RQ" -> SlurmStateFlag.Requeued,
      "REQUEUE_HOLD" -> SlurmStateFlag.RequeueHold,
      "RH" -> SlurmStateFlag.RequeueHold,
      "REQUEUE_FED" -> SlurmStateFlag.RequeueFederation,
      "RF" -> SlurmStateFlag.RequeueFederation,
      "SPECIAL_EXIT" -> SlurmStateFlag.SpecialExit,
      "SE" -> SlurmStateFlag.SpecialExit
    )

    states.foreach { case (raw, flag) =>
      val report = SlurmStateParser.report(raw)
      assertEquals(report.flags, Vector(flag), clues(raw))
      assertNotEquals(Terminality.of(report.state), Terminality.Terminal, clues(raw))
      val accounting =
        SacctParsable2.parse(evidence(s"1001|$raw|0:0|None\n"), expected).toOption.get.head
      assertNotEquals(Terminality.of(accounting.state), Terminality.Terminal, clues(raw))
      assertEquals(accounting.outcome, None, clues(raw))
    }
  }

  test("structured queue exposes requeueing without consulting raw fields") {
    val job = JobRef(JobId.from("2005").toOption.get, None)
    val raw =
      """{"jobs":[{"job_id":2005,"job_state":["REQUEUED"],"start_time":{"set":true,"infinite":false,"number":1784800800}}]}"""
    val observation =
      SqueueJsonV0043.parse(evidence(raw), NonEmptyVector.one(job)).toOption.get.head

    // The flag is what carries requeueing; the base state is honestly unknown.
    assertEquals(observation.flags, Vector(SlurmStateFlag.Requeued))
    assertEquals(
      InterruptionClass.classify(
        ReportedState(observation.state, observation.flags, truncated = false)
      ),
      InterruptionClass.Requeueing
    )
    assertEquals(
      observation.timing.start,
      Some(
        JobStart.Reported(
          SchedulerTimestamp.Absolute(Instant.ofEpochSecond(1784800800L))
        )
      )
    )
  }

  test("array observations and partial failures never alias sibling elements") {
    val parent = JobId.from("9100").toOption.get
    val first = JobRef(parent, Some(ArrayIndex.from(0).toOption.get))
    val second = JobRef(parent, Some(ArrayIndex.from(1).toOption.get))
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

  test("sacct command requests expanded allocation identities using JobID") {
    val parent = JobId.from("9100").toOption.get
    val jobs = NonEmptyVector.of(
      JobRef(parent, Some(ArrayIndex.from(0).toOption.get)),
      JobRef(parent, Some(ArrayIndex.from(1).toOption.get))
    )
    val command = SlurmCommands.accounting(jobs)

    assert(command.arguments.contains("--array"))
    assert(command.arguments.contains("--allocations"))
    assert(command.arguments.contains("--format=JobID,State,ExitCode,Reason"))
    assert(!command.arguments.exists(_.contains("JobIDRaw")))
  }

  test("sacct malformed rows are a parse failure, never a silently absent job") {
    val parent = JobId.from("9100").toOption.get
    val expected = NonEmptyVector.fromVectorUnsafe(
      (0 to 4).toVector.map(index => JobRef(parent, Some(ArrayIndex.from(index).toOption.get)))
    )
    val directory = "/fixtures/sacct-array-contract-v1"
    // The fixture carries a requested job whose exit code is unreadable, plus a row with no
    // delimiters at all. Reporting 9100_4 as merely missing would claim Slurm has no record of a
    // job whose record we simply failed to read.
    val parsed = SacctParsable2.parse(evidence(resource(s"$directory/stdout.txt")), expected)
    val problems = parsed.left.toOption.map(_.toVector).getOrElse(Vector.empty)

    assert(parsed.isLeft, s"malformed rows must fail the response, got $parsed")
    assertEquals(
      problems.map(_.code).sorted,
      Vector("invalid-accounting-exit-code", "invalid-accounting-row")
    )
    assert(
      problems.exists(_.fields.get("row").exists(_.contains("9100_4"))),
      s"the failure must name the offending row, got $problems"
    )

    val provenance =
      io.circe.parser.parse(resource(s"$directory/provenance.json")).toOption.get.hcursor
    val bytes = resourceBytes(s"$directory/stdout.txt")
    assertEquals(provenance.get[String]("contractSource").toOption, Some("schedmd-sacct-manual"))
    assertEquals(provenance.get[Boolean]("clusterCapture").toOption, Some(false))
    assertEquals(
      provenance.downField("stdout").get[Long]("bytes").toOption,
      Some(bytes.length.toLong)
    )
    assertEquals(provenance.downField("stdout").get[String]("sha256").toOption, Some(sha256(bytes)))
  }

  test("well-formed sacct rows still isolate summaries, steps, and unrelated jobs") {
    val parent = JobId.from("9100").toOption.get
    val expected = NonEmptyVector.fromVectorUnsafe(
      (0 to 4).toVector.map(index => JobRef(parent, Some(ArrayIndex.from(index).toOption.get)))
    )
    val output =
      """9100_[0-7%2]|PENDING|0:0|None
        |9100_0|COMPLETED|0:0|None
        |9100_0.batch|COMPLETED|0:0|None
        |9100_1|OUT_OF_MEMORY|0:9|OutOfMemory
        |9100_1.extern|COMPLETED|0:0|None
        |9100_2|TIMEOUT|0:15|TimeLimit
        |9100_3|FUTURE_STATE|Unknown|FutureReason
        |9999|FAILED|2:0|UnrelatedJob
        |""".stripMargin

    val records = SacctParsable2.parse(evidence(output), expected).toOption.get
    val missing = expected.toVector.filterNot(records.map(_.job).toSet)

    assertEquals(
      records.map(_.job.arrayIndex.map(_.value)),
      Vector(Some(0), Some(1), Some(2), Some(3))
    )
    assertEquals(records(1).outcome, Some(WorkloadOutcome.OutOfMemory))
    assertEquals(records(2).outcome, Some(WorkloadOutcome.TimeLimitExceeded))
    assertEquals(records(3).state, SlurmState.Unknown("FUTURE_STATE"))
    assertEquals(records(3).exitStatus, None)
    // 9100_4 has no row at all here, so genuine absence is still reported as absence.
    assertEquals(missing, Vector(expected.last))
  }

  test("a reason containing the field delimiter stays one readable row") {
    val job = JobId.from("9200").toOption.get
    val expected = NonEmptyVector.one(JobRef(job, None))
    val output = "9200|FAILED|1:0|JobLaunchFailure: exec failed | see slurmd log\n"

    val records = SacctParsable2.parse(evidence(output), expected).toOption.get
    assertEquals(records.size, 1)
    assertEquals(
      records.head.rawFields.get("Reason"),
      Some("JobLaunchFailure: exec failed | see slurmd log")
    )
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
        JobRef(job, Some(ArrayIndex.from(0).toOption.get)),
        JobRef(job, Some(ArrayIndex.from(1).toOption.get))
      )
      val parsed = VersionedSqueueParsers.parse(parser, evidence(resource(path)), expected)
      assert(parsed.isRight, clues(family, parsed))
      assertEquals(parsed.toOption.get.map(_.job.arrayIndex), expected.toVector.map(_.arrayIndex))
    }
  }

  test("actual Slurm 25.05.6 compressed array output is parsed without unbounded expansion") {
    val parent = JobId.from("1").toOption.get
    val expected = NonEmptyVector.of(
      JobRef(parent, Some(ArrayIndex.from(0).toOption.get)),
      JobRef(parent, Some(ArrayIndex.from(1).toOption.get))
    )
    val directory = "/fixtures/slurm-25.05.6-v0.0.43-actual"
    val parsed = VersionedSqueueParsers.parse(
      DataParserVersion.from("v0.0.43").toOption.get,
      evidence(resource(s"$directory/stdout.json")),
      expected
    )

    assert(parsed.isRight, parsed)
    val cluster = ClusterName.from("cluster").toOption.get
    // The cluster the site reports is retained as evidence and kept out of identity.
    assertEquals(parsed.toOption.get.map(_.job), expected.toVector)
    assert(parsed.toOption.get.forall(_.reportedCluster.contains(cluster)))
    assert(parsed.toOption.get.forall(_.state == SlurmState.Pending))
    assert(parsed.toOption.get.forall(_.timing.start.isEmpty))
    assert(parsed.toOption.get.forall(_.timing.timeLimit == ObservedTimeLimit.Unlimited))

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
    val expected = NonEmptyVector.one(JobRef(JobId.from("9200").toOption.get, None))
    val raw =
      """{"jobs":[{"job_id":9200,"array_job_id":{"set":false,"infinite":false,"number":0},"array_task_id":{"set":false,"infinite":false,"number":0},"job_state":["RUNNING"]}]}"""

    val parsed = SqueueJsonV0043.parse(evidence(raw), expected).toOption.get

    assertEquals(parsed.map(_.job), Vector(expected.head))
  }

  test("a compressed pending array record expands only requested matching elements") {
    val parent = JobId.from("9300").toOption.get
    val expected = NonEmptyVector.of(
      JobRef(parent, Some(ArrayIndex.from(1).toOption.get)),
      JobRef(parent, Some(ArrayIndex.from(2).toOption.get)),
      JobRef(parent, Some(ArrayIndex.from(7).toOption.get))
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
      ByteVector.view(value.getBytes(StandardCharsets.UTF_8))
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
