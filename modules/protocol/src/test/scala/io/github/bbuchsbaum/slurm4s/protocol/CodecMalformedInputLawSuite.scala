package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.ObservationBatch
import io.github.bbuchsbaum.slurm4s.core.SchedulerQueryResult

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import scala.util.Try

/** Malformed-input laws for the hand-written wire codecs (P6f.34).
  *
  * A decoder reads bytes chosen by a peer, so "returns a Left" and "throws" are very different
  * outcomes: the first is a protocol error the caller can report, the second escapes the typed
  * failure channel entirely and takes down whatever was reading the frame. Every decoder here must
  * fail as a value.
  *
  * Both halves matter. Arbitrary JSON checks the outer shape, but it usually fails at the first
  * field and so never reaches the interesting code. Mutating a *valid* encoding — dropping a field,
  * swapping a type, emptying a collection — reaches decoders that have already gotten past their
  * discriminator, which is where partial reads and unchecked casts actually live.
  */
class CodecMalformedInputLawSuite extends munit.ScalaCheckSuite:
  import ProtocolGenerators.*
  import JsonCorpus.*

  // SchedulerQueryResult is the one enum in this family that does not `derive CanEqual`, so a
  // comparison needs the instance supplied here. Test-local on purpose: widening the public type's
  // equality is a decision for the codec-ownership work, not for a law suite.
  given observationQueryCanEqual: CanEqual[
    SchedulerQueryResult[ObservationBatch],
    SchedulerQueryResult[ObservationBatch]
  ] = CanEqual.derived

  private def neverThrows[A](name: String, json: Json, decode: Json => Either[Any, A])(using
      munit.Location
  ): Boolean =
    Try(decode(json)) match
      case scala.util.Success(_)     => true
      case scala.util.Failure(error) =>
        fail(
          s"$name threw ${error.getClass.getName} instead of returning a Left" +
            s" for ${json.noSpaces.take(300)}"
        )

  /** Every hand-written entry point that takes a bare `Json`, decoded by name for the failure text.
    */
  private val decoders: Vector[(String, Json => Either[Any, Any])] = Vector(
    "decodeSubmitRequest" -> AgentDomainJson.decodeSubmitRequest,
    "decodeJobRef" -> AgentDomainJson.decodeJobRef,
    "decodeJobRefs" -> AgentDomainJson.decodeJobRefs,
    "decodeEvidence" -> AgentDomainJson.decodeEvidence,
    "decodeLogRequest" -> AgentDomainJson.decodeLogRequest,
    "decodeCapabilities" -> AgentDomainJson.decodeCapabilities,
    "decodeSubmission" -> AgentDomainJson.decodeSubmission,
    "decodeObservation" -> AgentDomainJson.decodeObservation,
    "decodeAccounting" -> AgentDomainJson.decodeAccounting,
    "decodeCancellation" -> AgentDomainJson.decodeCancellation,
    "decodeLogResult" -> AgentDomainJson.decodeLogResult,
    "decodeRemoteTaskRequest" -> AgentDomainJson.decodeRemoteTaskRequest,
    "decodeRemoteSubmission" -> AgentDomainJson.decodeRemoteSubmission,
    "decodeRemoteBatchRequest" -> AgentDomainJson.decodeRemoteBatchRequest,
    "decodeRemoteBatchSubmission" -> AgentDomainJson.decodeRemoteBatchSubmission,
    "decodeRemoteScriptBatchRequest" -> AgentDomainJson.decodeRemoteScriptBatchRequest,
    "decodeRemoteScriptBatchSubmission" -> AgentDomainJson.decodeRemoteScriptBatchSubmission,
    "decodeRemoteScriptExitReadRequest" -> AgentDomainJson.decodeRemoteScriptExitReadRequest,
    "decodeRemoteScriptExitRead" -> AgentDomainJson.decodeRemoteScriptExitRead,
    "HandshakeJson.decodeRequest" -> HandshakeJson.decodeRequest,
    "HandshakeJson.decodeResponse" -> HandshakeJson.decodeResponse
  )

  property("no decoder throws on arbitrary JSON") {
    forAll(arbitraryJson) { json =>
      decoders.forall((name, decode) => neverThrows(name, json, decode))
    }
  }

  /** Valid encodings from across the codec surface, so mutation starts from something decodable. */
  private val validEncoding: Gen[Json] =
    Gen.oneOf(
      jobRef.map(AgentDomainJson.encodeJobRef),
      evidenceBundle.map(AgentDomainJson.encodeEvidence),
      schedulerQueryResult(observationBatch).map(AgentDomainJson.encodeObservation),
      schedulerQueryResult(accountingBatch).map(AgentDomainJson.encodeAccounting),
      schedulerQueryResult(schedulerCapabilities).map(AgentDomainJson.encodeCapabilities),
      submissionAttempt.map(AgentDomainJson.encodeSubmission),
      cancellationAttempt.map(AgentDomainJson.encodeCancellation),
      logReadResult.map(AgentDomainJson.encodeLogResult)
    )

  property("no decoder throws on a corrupted but structurally plausible encoding") {
    forAll(validEncoding.flatMap(mutations)) { json =>
      decoders.forall((name, decode) => neverThrows(name, json, decode))
    }
  }

  /** Forward compatibility, and the "unknown-field behavior" half of the bead's acceptance.
    *
    * An older reader must tolerate a field a newer writer added, and must decode to exactly what it
    * would have decoded without it. Rejecting the message would make every additive protocol change
    * a breaking one.
    */
  property("an unknown field added by a newer peer is ignored, not rejected") {
    forAll(schedulerQueryResult(observationBatch)) { value =>
      val encoded = AgentDomainJson.encodeObservation(value)
      withUnknownField(encoded).sample.forall { extended =>
        AgentDomainJson.decodeObservation(extended) == Right(value)
      }
    }
  }

  /** Guards the properties above against passing vacuously.
    *
    * If every generated input were rejected at the discriminator, "no decoder throws" would be true
    * but would say nothing about the field-reading code. The unknown-field corpus must decode
    * successfully (that is its point, and it proves the corpus traverses a complete decode), while
    * the corruption corpus must be rejected (which proves it is really corrupting something).
    */
  test("the malformed corpora reach full decodes as well as discriminator rejections") {
    val valid = Vector
      .fill(200)(schedulerQueryResult(observationBatch).sample)
      .flatten
      .map(value => (value, AgentDomainJson.encodeObservation(value)))
    assert(valid.sizeIs > 0, "no observations were generated")

    val extended = valid.flatMap((value, json) => withUnknownField(json).sample.map((value, _)))
    assert(
      extended.exists((value, json) => AgentDomainJson.decodeObservation(json) == Right(value)),
      "no unknown-field input decoded successfully, so the corpus never exercises a full decode"
    )

    val corrupted = valid.flatMap((_, json) => mutations(json).sample)
    assert(
      corrupted.exists(json => AgentDomainJson.decodeObservation(json).isLeft),
      "no corrupted input was rejected, so the mutations are not corrupting anything"
    )
  }

  property("no decoder throws when a nested field is corrupted rather than a top-level one") {
    forAll(validEncoding.flatMap(nestedMutations)) { json =>
      decoders.forall((name, decode) => neverThrows(name, json, decode))
    }
  }
