package io.github.bbuchsbaum.slurm4s.protocol

import cats.data.NonEmptyVector
import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.AccountingBatch
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.CompletionExitStatus
import io.github.bbuchsbaum.slurm4s.core.SchedulerQueryResult
import io.github.bbuchsbaum.slurm4s.core.WorkloadOutcome

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** Round-trip laws for the hand-written wire codecs (P6f.34).
  *
  * `decode(encode(x)) == Right(x)` is the one check that fails automatically when a codec stops
  * carrying a field. The audit on 2026-08-03 found `JobObservation.reportedCluster` dropped, and
  * found it by reading every codec against every case class by hand — a snapshot that cannot catch
  * the next occurrence. The failure mode it exposed is structural: a field with a default lets the
  * decoder omit it and still typecheck under `-Werror`, so nothing but a property over populated
  * values will notice.
  *
  * These laws are therefore only as good as `ProtocolGenerators`, which is written to put a
  * non-default value in every defaulted field. `CodecLawStrengthSuite` is the guard on that
  * assumption: it proves these laws fail against a codec that drops a defaulted field.
  */
class CodecRoundTripLawSuite extends munit.ScalaCheckSuite:
  import ProtocolGenerators.*

  /** Names the value on failure. ScalaCheck reports the generated input, but the encoded JSON is
    * what tells you which field went missing, and it is not recoverable from the input alone.
    */
  private def roundTrips[A](value: A, encoded: Json, decoded: Either[String, A])(using
      munit.Location
  ): Unit =
    assertEquals(
      decoded,
      Right(value),
      s"round trip lost information; encoded as ${encoded.noSpaces}"
    )

  property("a job reference round-trips") {
    forAll(jobRef) { value =>
      roundTrips(
        value,
        AgentDomainJson.encodeJobRef(value),
        AgentDomainJson.decodeJobRef(AgentDomainJson.encodeJobRef(value))
      )
    }
  }

  /** The submit request carries defaulted environment, array, retry-safety, and termination-notice
    * fields, which makes it the most exposed instance of the hazard these laws exist for.
    */
  property("a submit request round-trips, including all defaulted fields") {
    forAll(exitOnlyLaunchSpec) { value =>
      val encoded = AgentDomainJson.encodeSubmitRequest(value)
      assertEquals(encoded.flatMap(AgentDomainJson.decodeSubmitRequest), Right(value))
    }
  }

  /** The dangerous failure mode is not refusal but silent downgrade: a contract that promised
    * declared outputs, encoded as exit-only, would let the job report success while its outputs
    * were never collected.
    */
  property("a contract the submit protocol cannot express is refused, not downgraded") {
    forAll(unsupportedContractLaunchSpec) { value =>
      AgentDomainJson.encodeSubmitRequest(value) match
        case Left(_)        => ()
        case Right(encoded) =>
          fail(
            s"encoded a ${value.resultContract.mode} contract as ${encoded.noSpaces}, " +
              "so the declared outputs silently became exit-only"
          )
    }
  }

  property("a job reference collection round-trips, preserving order") {
    forAll(Gen.zip(jobRef, Gen.listOf(jobRef))) { (head, rest) =>
      val value = NonEmptyVector(head, rest.toVector)
      val encoded = AgentDomainJson.encodeJobRefs(value)
      roundTrips(value, encoded, AgentDomainJson.decodeJobRefs(encoded))
    }
  }

  property("an evidence bundle round-trips, including its related captures") {
    forAll(evidenceBundle) { value =>
      val encoded = AgentDomainJson.encodeEvidence(value)
      roundTrips(value, encoded, AgentDomainJson.decodeEvidence(encoded))
    }
  }

  property("snapshot-facing domain values round-trip through their owned codecs") {
    forAll(
      Gen.zip(
        diagnostics,
        workloadOutcome,
        observationResult,
        accountingRecord
      )
    ) { (problems, outcome, observation, accounting) =>
      assertEquals(
        AgentDomainJson.decodeDiagnostics(AgentDomainJson.encodeDiagnostics(problems)),
        Right(problems)
      )
      assertEquals(
        AgentDomainJson.decodeWorkloadOutcome(AgentDomainJson.encodeWorkloadOutcome(outcome)),
        Right(outcome)
      )
      assertEquals(
        AgentDomainJson.decodeObservationResult(
          AgentDomainJson.encodeObservationResult(observation)
        ),
        Right(observation)
      )
      assertEquals(
        AgentDomainJson.decodeAccountingRecord(
          AgentDomainJson.encodeAccountingRecord(accounting)
        ),
        Right(accounting)
      )
    }
  }

  property("an observation query result round-trips every case, not only success") {
    forAll(schedulerQueryResult(observationBatch)) { value =>
      val encoded = AgentDomainJson.encodeObservation(value)
      roundTrips(value, encoded, AgentDomainJson.decodeObservation(encoded))
    }
  }

  property("an accounting query result round-trips, including the missing-job list") {
    forAll(schedulerQueryResult(accountingBatch)) { value =>
      val encoded = AgentDomainJson.encodeAccounting(value)
      roundTrips(value, encoded, AgentDomainJson.decodeAccounting(encoded))
    }
  }

  property("completed accounting requires an explicit zero or null exitCode field") {
    forAll(accountingRecord) { record =>
      val value: SchedulerQueryResult[AccountingBatch] =
        SchedulerQueryResult.Succeeded(
          AccountingBatch(
            NonEmptyVector.one(
              record.copy(
                outcome = Some(
                  WorkloadOutcome.Completed(CompletionExitStatus.ReportedZero)
                )
              )
            ),
            Vector.empty
          )
        )
      val encoded = AgentDomainJson.encodeAccounting(value)
      val missing = transformCompleted(encoded)(_.remove("exitCode"))
      val nonZero = transformCompleted(encoded)(_.add("exitCode", Json.fromInt(42)))

      assert(AgentDomainJson.decodeAccounting(missing).isLeft)
      assert(AgentDomainJson.decodeAccounting(nonZero).isLeft)
    }
  }

  property("a submission attempt round-trips every acceptance and failure case") {
    forAll(submissionAttempt) { value =>
      val encoded = AgentDomainJson.encodeSubmission(value)
      roundTrips(value, encoded, AgentDomainJson.decodeSubmission(encoded))
    }
  }

  property("a cancellation attempt round-trips") {
    forAll(cancellationAttempt) { value =>
      val encoded = AgentDomainJson.encodeCancellation(value)
      roundTrips(value, encoded, AgentDomainJson.decodeCancellation(encoded))
    }
  }

  property("a capabilities query result round-trips its raw evidence") {
    forAll(schedulerQueryResult(schedulerCapabilities)) { value =>
      val encoded = AgentDomainJson.encodeCapabilities(value)
      roundTrips(value, encoded, AgentDomainJson.decodeCapabilities(encoded))
    }
  }

  property("a log read result round-trips, including high-bit bytes in a page") {
    forAll(logReadResult) { value =>
      val encoded = AgentDomainJson.encodeLogResult(value)
      roundTrips(value, encoded, AgentDomainJson.decodeLogResult(encoded))
    }
  }

  /** The persisted codecs. A field dropped here is data lost on disk rather than on a wire that can
    * be retried, and the journal is what a controller replays after a crash.
    */
  property("a result envelope round-trips through its persisted encoding") {
    forAll(resultEnvelope) { value =>
      val encoded = ResultEnvelopeCodec
        .encode(value, ByteLimit.maximumCommandCapture, ByteLimit.maximumCommandCapture)
      assertEquals(
        encoded.flatMap(
          ResultEnvelopeCodec
            .decode(_, ByteLimit.maximumCommandCapture, ByteLimit.maximumCommandCapture)
        ),
        Right(value)
      )
    }
  }

  property("a durable result handle round-trips, including its retry-safety field") {
    forAll(durableResultHandle) { value =>
      val encoded = DurableResultHandleCodec.encode(value, ByteLimit.maximumCommandCapture)
      assertEquals(
        encoded.flatMap(DurableResultHandleCodec.decode(_, ByteLimit.maximumCommandCapture)),
        Right(value)
      )
    }
  }

  property("a log request round-trips its reference, cursor and limit") {
    forAll(Gen.zip(logRef, logCursor, Gen.choose(1, 1 << 20))) { (ref, cursor, limit) =>
      val bound = ByteLimit.from(limit).toOption.get
      val encoded = AgentDomainJson.encodeLogRequest(ref, cursor, bound)
      roundTrips((ref, cursor, bound), encoded, AgentDomainJson.decodeLogRequest(encoded))
    }
  }

  private def transformCompleted(json: Json)(
      change: io.circe.JsonObject => io.circe.JsonObject
  ): Json =
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(transformCompleted(_)(change))),
      fields =>
        Json.obj(fields.toVector.map {
          case ("Completed", payload) => "Completed" -> payload.mapObject(change)
          case (name, value)          => name -> transformCompleted(value)(change)
        }*)
    )
