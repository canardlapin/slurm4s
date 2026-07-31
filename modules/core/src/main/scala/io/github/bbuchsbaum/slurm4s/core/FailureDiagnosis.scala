package io.github.bbuchsbaum.slurm4s.core

import cats.data.NonEmptyVector

import java.nio.charset.StandardCharsets
import java.time.Instant

final case class LogByteRange private (
    start: LogOffset,
    endExclusive: LogOffset
) derives CanEqual

object LogByteRange:
  def from(
      start: LogOffset,
      endExclusive: LogOffset
  ): Either[ValidationFailure, LogByteRange] =
    Either.cond(
      endExclusive.value >= start.value,
      LogByteRange(start, endExclusive),
      ValidationFailure("logByteRange", "end must not precede start")
    )

enum LogHintKind derives CanEqual:
  case PythonTraceback
  case RExecutionError
  case JvmException
  case FileNotFound
  case MemoryPressure
  case ProcessKilled

  private[core] def stablePriority: Int = this match
    case PythonTraceback => 0
    case RExecutionError => 1
    case JvmException    => 2
    case FileNotFound    => 3
    case MemoryPressure  => 4
    case ProcessKilled   => 5

final case class LogExcerpt private[core] (
    ref: LogRef,
    matched: LogByteRange,
    retained: LogByteRange,
    bytes: Vector[Byte],
    observedAt: Instant
) derives CanEqual

final case class LogHint(kind: LogHintKind, excerpt: LogExcerpt) derives CanEqual

final case class LogRecognitionLimits(
    maximumHints: PositiveInt,
    maximumExcerptBytes: ByteLimit
) derives CanEqual

object CommonLogRecognizer:
  final private case class Pattern(kind: LogHintKind, needle: Vector[Byte])

  private val patterns = Vector(
    pattern(LogHintKind.MemoryPressure, "out of memory"),
    pattern(LogHintKind.MemoryPressure, "oom-kill"),
    pattern(LogHintKind.PythonTraceback, "traceback (most recent call last)"),
    pattern(LogHintKind.RExecutionError, "error in "),
    pattern(LogHintKind.RExecutionError, "execution halted"),
    pattern(LogHintKind.JvmException, "exception in thread"),
    pattern(LogHintKind.FileNotFound, "no such file or directory"),
    pattern(LogHintKind.ProcessKilled, "killed")
  )

  def recognize(
      ref: LogRef,
      page: LogPage,
      limits: LogRecognitionLimits
  ): Either[ValidationFailure, Vector[LogHint]] =
    val pageStart = page.next.offset.value - page.bytes.size.toLong
    Either
      .cond(
        pageStart >= 0L,
        pageStart,
        ValidationFailure(
          "logPage",
          "next cursor offset must cover every byte in the page"
        )
      )
      .flatMap { validPageStart =>
        val lowered = page.bytes.map(asciiLower)
        val unique = patterns
          .flatMap { candidate =>
            val index = indexOf(lowered, candidate.needle)
            Option.when(index >= 0)(candidate -> index)
          }
          .groupBy(_._1.kind)
          .valuesIterator
          .map(_.minBy(_._2))
          .toVector
          .sortBy { case (candidate, index) => index -> candidate.kind.stablePriority }
          .take(limits.maximumHints.toInt)

        unique.foldLeft[Either[ValidationFailure, Vector[LogHint]]](Right(Vector.empty)) {
          case (result, (candidate, index)) =>
            result.flatMap { hints =>
              val matchStart = validPageStart + index.toLong
              val matchEnd = matchStart + candidate.needle.size.toLong
              val retained = retainedSlice(
                page.bytes.size,
                index,
                candidate.needle.size,
                limits.maximumExcerptBytes.value
              )
              val retainedStart = validPageStart + retained._1.toLong
              val retainedEnd = validPageStart + retained._2.toLong
              for
                matched <- range(matchStart, matchEnd)
                retainedRange <- range(retainedStart, retainedEnd)
              yield hints :+ LogHint(
                candidate.kind,
                LogExcerpt(
                  ref,
                  matched,
                  retainedRange,
                  page.bytes.slice(retained._1, retained._2),
                  page.observedAt
                )
              )
            }
        }
      }

  private def pattern(kind: LogHintKind, value: String): Pattern =
    Pattern(kind, value.getBytes(StandardCharsets.US_ASCII).toVector)

  private def asciiLower(value: Byte): Byte =
    if value >= 'A'.toByte && value <= 'Z'.toByte then (value + ('a' - 'A')).toByte
    else value

  private def indexOf(bytes: Vector[Byte], needle: Vector[Byte]): Int =
    if needle.isEmpty then 0
    else
      var start = 0
      var found = -1
      while found < 0 && start <= bytes.size - needle.size do
        var offset = 0
        while offset < needle.size && bytes(start + offset) == needle(offset) do offset += 1
        if offset == needle.size then found = start
        else start += 1
      found

  private def retainedSlice(
      pageSize: Int,
      matchStart: Int,
      matchSize: Int,
      maximum: Int
  ): (Int, Int) =
    val retainedSize = math.min(pageSize, maximum)
    val preferredStart = math.max(0, matchStart - math.max(0, (retainedSize - matchSize) / 2))
    val end = math.min(pageSize, preferredStart + retainedSize)
    math.max(0, end - retainedSize) -> end

  private def range(
      start: Long,
      endExclusive: Long
  ): Either[ValidationFailure, LogByteRange] =
    for
      startOffset <- LogOffset.from(start)
      endOffset <- LogOffset.from(endExclusive)
      range <- LogByteRange.from(startOffset, endOffset)
    yield range

enum DiagnosisEvidence derives CanEqual:
  case Submission(value: EvidenceBundle)
  case Scheduler(value: EvidenceBundle)
  case Accounting(value: EvidenceBundle)
  case Worker(value: WorkerEvent)
  case Result(value: EvidenceBundle)
  case Diagnostics(value: io.github.bbuchsbaum.slurm4s.core.Diagnostics)
  case Log(value: LogHint)

final case class Candidate[+A](
    value: A,
    evidence: NonEmptyVector[DiagnosisEvidence]
)

enum Assessment[+A]:
  case Confirmed(value: A, evidence: NonEmptyVector[DiagnosisEvidence])
  case Suspected(candidates: NonEmptyVector[Candidate[A]])
  case Undetermined(evidence: Vector[DiagnosisEvidence])

final case class DiagnosisInput private (
    submission: Option[SubmissionAttempt],
    observation: Option[ObservationResult],
    accounting: Option[AccountingRecord],
    workerEvents: Vector[WorkerEvent],
    result: Option[ExecutionResult[?]],
    logHints: Vector[LogHint]
)

object DiagnosisInput:
  def from(
      submission: Option[SubmissionAttempt] = None,
      observation: Option[ObservationResult] = None,
      accounting: Option[AccountingRecord] = None,
      workerEvents: Vector[WorkerEvent] = Vector.empty,
      result: Option[ExecutionResult[?]] = None,
      logHints: Vector[LogHint] = Vector.empty,
      maximumEventAndHintItems: PositiveInt
  ): Either[ValidationFailure, DiagnosisInput] =
    val actual = workerEvents.size.toLong + logHints.size.toLong
    Either.cond(
      actual <= maximumEventAndHintItems.toInt.toLong,
      DiagnosisInput(submission, observation, accounting, workerEvents, result, logHints),
      ValidationFailure(
        "diagnosisEvidence",
        s"contains $actual worker events and log hints; maximum is ${maximumEventAndHintItems.toInt}"
      )
    )

object FailureDiagnosis:
  def apply(
      primary: FailureCause,
      confirmedContributors: Vector[FailureCause],
      suspectedContributors: Vector[FailureCause]
  ): FailureDiagnosis =
    io.github.bbuchsbaum.remoteexec.kernel.FailureDiagnosis(
      primary,
      confirmedContributors,
      suspectedContributors
    )

  def unapply(
      diagnosis: FailureDiagnosis
  ): Some[(FailureCause, Vector[FailureCause], Vector[FailureCause])] =
    Some(
      (
        diagnosis.primary,
        diagnosis.confirmedContributors,
        diagnosis.suspectedContributors
      )
    )

  private enum Confidence derives CanEqual:
    case Confirmed
    case Suspected

  final private case class Finding(
      cause: FailureCause,
      evidence: DiagnosisEvidence,
      confidence: Confidence,
      authority: Int
  )

  def assess(input: DiagnosisInput): Assessment[FailureDiagnosis] =
    val findings =
      submissionFindings(input.submission) ++
        observationFindings(input.observation) ++
        accountingFindings(input.accounting) ++
        workerFindings(input.workerEvents) ++
        resultFindings(input.result) ++
        logFindings(input.logHints)
    val allEvidence = (
      baseEvidence(input) ++ findings.map(_.evidence)
    ).distinct
    val confirmed = ordered(findings.filter(_.confidence == Confidence.Confirmed))
    val suspected = ordered(findings.filter(_.confidence == Confidence.Suspected))

    NonEmptyVector.fromVector(confirmed) match
      case Some(values) =>
        val primary = values.head.cause
        val diagnosis = FailureDiagnosis(
          primary,
          distinctCauses(values.tail.map(_.cause).filterNot(_ == primary)),
          distinctCauses(suspected.map(_.cause).filterNot(_ == primary))
        )
        Assessment.Confirmed(
          diagnosis,
          NonEmptyVector
            .fromVector(allEvidence)
            .getOrElse(
              NonEmptyVector.one(values.head.evidence)
            )
        )
      case None =>
        val grouped = suspected
          .groupBy(_.cause)
          .toVector
          .sortBy { case (cause, values) =>
            (-values.map(_.authority).max, cause.deterministicKey)
          }
          .map { case (cause, values) =>
            Candidate(
              FailureDiagnosis(cause, Vector.empty, Vector.empty),
              NonEmptyVector.fromVectorUnsafe(values.map(_.evidence).distinct)
            )
          }
        NonEmptyVector
          .fromVector(grouped)
          .fold[Assessment[FailureDiagnosis]](Assessment.Undetermined(allEvidence))(
            Assessment.Suspected.apply
          )

  private def submissionFindings(value: Option[SubmissionAttempt]): Vector[Finding] =
    value.toVector.flatMap {
      case SubmissionAttempt.Completed(Submission.Rejected(diagnostics, evidence)) =>
        Vector(
          confirmed(
            FailureCause.SubmissionRejected(codes(diagnostics)),
            DiagnosisEvidence.Submission(evidence),
            70
          )
        )
      case SubmissionAttempt.InvocationFailed(
            InvocationResult.SpawnFailed(kind, _, evidence)
          ) =>
        Vector(
          confirmed(
            FailureCause.ProgramLaunchFailed(kind),
            DiagnosisEvidence.Submission(evidence),
            70
          )
        )
      case SubmissionAttempt.PreparationFailed(diagnostics) =>
        Vector(
          confirmed(
            FailureCause.RequestPreparationFailed(codes(diagnostics)),
            DiagnosisEvidence.Diagnostics(diagnostics),
            70
          )
        )
      case _ => Vector.empty
    }

  private def observationFindings(value: Option[ObservationResult]): Vector[Finding] =
    value.toVector.flatMap {
      case ObservationResult.Observed(observation) =>
        stateCause(observation.state).toVector.map { cause =>
          observation.freshness match
            case _: Freshness.Current =>
              confirmed(cause, DiagnosisEvidence.Scheduler(observation.evidence), 80)
            case _ =>
              suspected(cause, DiagnosisEvidence.Scheduler(observation.evidence), 50)
        }
      case _ => Vector.empty
    }

  private def accountingFindings(value: Option[AccountingRecord]): Vector[Finding] =
    value.toVector.flatMap { record =>
      record.outcome.flatMap(outcomeCause).orElse(stateCause(record.state)).toVector.map { cause =>
        record.freshness match
          case _: Freshness.Current =>
            confirmed(cause, DiagnosisEvidence.Accounting(record.evidence), 100)
          case _ =>
            suspected(cause, DiagnosisEvidence.Accounting(record.evidence), 60)
      }
    }

  private def workerFindings(values: Vector[WorkerEvent]): Vector[Finding] =
    values.flatMap { event =>
      event.payload match
        case WorkerEventPayload.Failed(code, _) =>
          Vector(
            confirmed(
              FailureCause.WorkerFailure(code),
              DiagnosisEvidence.Worker(event),
              90
            )
          )
        case WorkerEventPayload.ProcessExited(exitCode) if exitCode != 0 =>
          Vector(
            confirmed(
              FailureCause.ProgramFailed(Some(exitCode), Vector.empty),
              DiagnosisEvidence.Worker(event),
              85
            )
          )
        case _ => Vector.empty
    }

  private def resultFindings(value: Option[ExecutionResult[?]]): Vector[Finding] =
    value.toVector.flatMap {
      case ExecutionResult.WorkloadFailed(outcome, evidence) =>
        outcomeCause(outcome).toVector.map(cause =>
          confirmed(cause, DiagnosisEvidence.Result(evidence), 95)
        )
      case ExecutionResult.ResultInvalid(diagnostics, evidence) =>
        Vector(
          confirmed(
            FailureCause.ResultInvalid(codes(diagnostics)),
            DiagnosisEvidence.Result(evidence),
            95
          )
        )
      case _ => Vector.empty
    }

  private def logFindings(values: Vector[LogHint]): Vector[Finding] =
    values.map { hint =>
      suspected(logCause(hint.kind), DiagnosisEvidence.Log(hint), 10)
    }

  private def baseEvidence(input: DiagnosisInput): Vector[DiagnosisEvidence] =
    input.submission.toVector.flatMap {
      case SubmissionAttempt.Completed(Submission.Accepted(_, evidence)) =>
        Vector(DiagnosisEvidence.Submission(evidence))
      case SubmissionAttempt.Completed(Submission.Rejected(diagnostics, evidence)) =>
        Vector(
          DiagnosisEvidence.Diagnostics(diagnostics),
          DiagnosisEvidence.Submission(evidence)
        )
      case SubmissionAttempt.Completed(Submission.AcceptanceUnknown(_, evidence)) =>
        Vector(DiagnosisEvidence.Submission(evidence))
      case SubmissionAttempt.InvocationFailed(result)       => invocationEvidence(result)
      case SubmissionAttempt.PreparationFailed(diagnostics) =>
        Vector(DiagnosisEvidence.Diagnostics(diagnostics))
    } ++ input.observation.toVector.flatMap {
      case ObservationResult.Observed(value) =>
        Vector(DiagnosisEvidence.Scheduler(value.evidence))
      case ObservationResult.NotFound(_, _, evidence) =>
        Vector(DiagnosisEvidence.Scheduler(evidence))
      case ObservationResult.Failed(_, _, diagnostics, evidence) =>
        Vector(
          DiagnosisEvidence.Diagnostics(diagnostics),
          DiagnosisEvidence.Scheduler(evidence)
        )
    } ++ input.accounting.toVector.map(value => DiagnosisEvidence.Accounting(value.evidence)) ++
      input.workerEvents.map(DiagnosisEvidence.Worker.apply) ++
      input.result.toVector.map {
        case ExecutionResult.Succeeded(_, _, evidence) =>
          DiagnosisEvidence.Result(evidence)
        case ExecutionResult.WorkloadFailed(_, evidence) =>
          DiagnosisEvidence.Result(evidence)
        case ExecutionResult.ResultInvalid(_, evidence) =>
          DiagnosisEvidence.Result(evidence)
        case ExecutionResult.Indeterminate(_, evidence) =>
          DiagnosisEvidence.Result(evidence)
      } ++ input.logHints.map(DiagnosisEvidence.Log.apply)

  private def invocationEvidence(value: InvocationResult): Vector[DiagnosisEvidence] =
    value match
      case InvocationResult.Exited(_, stdout, stderr) =>
        Vector(DiagnosisEvidence.Submission(EvidenceBundle(stdout, Vector(stderr))))
      case InvocationResult.SpawnFailed(_, diagnostics, evidence) =>
        Vector(
          DiagnosisEvidence.Diagnostics(diagnostics),
          DiagnosisEvidence.Submission(evidence)
        )
      case InvocationResult.TimedOut(_, stdout, stderr) =>
        Vector(DiagnosisEvidence.Submission(EvidenceBundle(stdout, Vector(stderr))))

  private def stateCause(value: SlurmState): Option[FailureCause] = value match
    case SlurmState.Failed      => Some(FailureCause.ProgramFailed(None, Vector.empty))
    case SlurmState.Cancelled   => Some(FailureCause.Cancelled)
    case SlurmState.OutOfMemory => Some(FailureCause.OutOfMemory)
    case SlurmState.TimedOut    => Some(FailureCause.TimeLimitExceeded)
    case SlurmState.NodeFailure => Some(FailureCause.NodeFailure)
    case SlurmState.Preempted   => Some(FailureCause.Preempted)
    // The node never came up, which is infrastructure rather than workload.
    case SlurmState.BootFail => Some(FailureCause.NodeFailure)
    // A deadline the scheduler enforced; the nearest neutral cause is a time constraint.
    case SlurmState.Deadline => Some(FailureCause.TimeLimitExceeded)
    case SlurmState.Pending | SlurmState.Running | SlurmState.Completed | SlurmState.Suspended |
        SlurmState.Unknown(_) =>
      None

  private def outcomeCause(value: WorkloadOutcome): Option[FailureCause] = value match
    case WorkloadOutcome.Completed(exitCode) if exitCode != 0 =>
      Some(FailureCause.ProgramFailed(Some(exitCode), Vector.empty))
    case WorkloadOutcome.Failed(exitCode, diagnostics) =>
      Some(FailureCause.ProgramFailed(exitCode, codes(diagnostics)))
    case WorkloadOutcome.OutOfMemory       => Some(FailureCause.OutOfMemory)
    case WorkloadOutcome.TimeLimitExceeded => Some(FailureCause.TimeLimitExceeded)
    case WorkloadOutcome.Cancelled         => Some(FailureCause.Cancelled)
    case WorkloadOutcome.NodeFailure       => Some(FailureCause.NodeFailure)
    case _                                 => None

  private def logCause(value: LogHintKind): FailureCause = value match
    case LogHintKind.PythonTraceback => FailureCause.RuntimeError("python")
    case LogHintKind.RExecutionError => FailureCause.RuntimeError("r")
    case LogHintKind.JvmException    => FailureCause.RuntimeError("jvm")
    case LogHintKind.FileNotFound    => FailureCause.FileNotFound
    case LogHintKind.MemoryPressure  => FailureCause.OutOfMemory
    case LogHintKind.ProcessKilled   => FailureCause.ProcessKilled

  private def confirmed(
      cause: FailureCause,
      evidence: DiagnosisEvidence,
      authority: Int
  ): Finding =
    Finding(cause, evidence, Confidence.Confirmed, authority)

  private def suspected(
      cause: FailureCause,
      evidence: DiagnosisEvidence,
      authority: Int
  ): Finding =
    Finding(cause, evidence, Confidence.Suspected, authority)

  private def ordered(values: Vector[Finding]): Vector[Finding] =
    values.sortBy(value => (-value.authority, value.cause.deterministicKey))

  private def distinctCauses(values: Vector[FailureCause]): Vector[FailureCause] =
    values.foldLeft(Vector.empty[FailureCause]) { (result, cause) =>
      if result.contains(cause) then result else result :+ cause
    }

  private def codes(value: Diagnostics): Vector[String] =
    value.toVector.map(_.code).distinct.sorted
