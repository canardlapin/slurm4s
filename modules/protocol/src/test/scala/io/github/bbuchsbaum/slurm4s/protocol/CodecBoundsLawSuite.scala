package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*

import org.scalacheck.Prop.forAll

import scodec.bits.ByteVector

import java.time.Instant

/** Bounds laws for the hand-written wire codecs (P6f.34).
  *
  * A decoder reads a size chosen by a peer, so the order of "check" and "allocate" is the whole
  * property. `Base64.getDecoder.decode` sizes its output array from its input, so a limit enforced
  * on the decoded result has already permitted the allocation it was meant to prevent. Frame-level
  * `maximumFrameBytes` was the only thing bounding two of these fields, which makes this
  * defence-in-depth rather than a fixed overflow — but the asymmetry between the agent and remote
  * paths for the *same field* was the real defect.
  *
  * Fixtures are built by encoding a real value and then substituting the one field under test,
  * rather than by hand-writing the JSON. The first draft hand-wrote it, guessed the evidence-source
  * shape wrong, and every oversize assertion passed because the fixture was malformed rather than
  * because a bound rejected it. Deriving the shape from the encoder makes that failure impossible
  * and keeps these tests honest if a shape ever changes.
  *
  * The oversize payloads are built as base64 text only, never as real byte arrays, so the suite
  * asserts rejection without allocating what it tests against.
  */
class CodecBoundsLawSuite extends munit.ScalaCheckSuite:
  import ProtocolGenerators.*

  private val smallEvidence: BoundedEvidence =
    BoundedEvidence.capture(EvidenceSource.AgentProtocol, Instant.EPOCH, ByteVector(1, 2, 3))

  /** Base64 text claiming more bytes than `limit` allows, without materializing them.
    *
    * Base64 spends 4 characters per 3 bytes, so this is the encoded length of `limit + 1` bytes. A
    * decoder that bounds before decoding rejects it having allocated nothing.
    */
  private def oversizeBase64(limit: ByteLimit): String =
    val characters = ((limit.value.toLong + 1L + 2L) / 3L) * 4L
    "A".repeat(characters.toInt)

  /** Replaces every `bytesBase64` field at any depth, leaving the rest of the shape as encoded.
    *
    * `originalByteCount` moves with it. Evidence carries the invariant that the original count
    * covers the retained bytes, so substituting only the payload would produce a message rejected
    * for violating that invariant — which looks identical to a size rejection through `isLeft` and
    * would let this suite pass with no size bound in place at all. Raising both leaves the size
    * limit as the only thing left to reject.
    */
  private def substituteBytes(json: Json, encoded: String): Json =
    val claimedBytes = (encoded.length.toLong / 4L) * 3L
    json.arrayOrObject(
      json,
      values => Json.fromValues(values.map(substituteBytes(_, encoded))),
      obj =>
        Json
          .fromJsonObject(
            obj.mapValues { value =>
              if value.isString || value.isNumber then value
              else substituteBytes(value, encoded)
            }
          )
          .mapObject { mapped =>
            if !mapped.contains("bytesBase64") then mapped
            else
              mapped
                .add("bytesBase64", Json.fromString(encoded))
                .add("originalByteCount", Json.fromLong(claimedBytes))
          }
    )

  /** Requires the rejection to name the limit, so a message rejected for some unrelated reason
    * cannot be mistaken for the bound doing its job.
    */
  private def rejects(name: String, result: Either[Any, Any])(using munit.Location): Unit =
    result match
      case Right(_)      => fail(s"$name accepted a payload past its limit")
      case Left(problem) =>
        val reason = problem.toString
        assert(
          // Covers both "exceeds ... limit" for byte bounds and "exceed ... entries" for cardinality.
          reason.contains("exceed"),
          s"$name rejected the payload, but not for its size: $reason"
        )

  private def accepts(name: String, result: Either[Any, Any])(using munit.Location): Unit =
    assert(result.isRight, s"$name rejected a payload within its limit: fixture shape is wrong")

  test("evidence bytes past the capture limit are rejected on the agent path") {
    val real = AgentDomainJson.encodeEvidence(EvidenceBundle(smallEvidence))
    accepts("decodeEvidence", AgentDomainJson.decodeEvidence(real))
    rejects(
      "decodeEvidence",
      AgentDomainJson.decodeEvidence(
        substituteBytes(real, oversizeBase64(ByteLimit.maximumCommandCapture))
      )
    )
  }

  test("a log page past the page limit is rejected") {
    val page = LogPage(ByteVector(1, 2, 3), LogCursor.start, endOfFile = true, Instant.EPOCH)
    val real = AgentDomainJson.encodeLogResult(LogReadResult.Page(page))
    accepts("decodeLogResult", AgentDomainJson.decodeLogResult(real))
    rejects(
      "decodeLogResult",
      AgentDomainJson.decodeLogResult(
        substituteBytes(real, oversizeBase64(ByteLimit.maximumLogPage))
      )
    )
  }

  /** The asymmetry this work was filed on: one field, bounded on the remote path and not the agent
    * one. Both must now reject the same payload.
    */
  test("the agent and remote evidence paths agree that oversize evidence is rejected") {
    val oversize = oversizeBase64(ByteLimit.maximumCommandCapture)

    val agentReal = AgentDomainJson.encodeEvidence(EvidenceBundle(smallEvidence))
    val remoteReal = AgentDomainJson.encodeRemoteScriptExitRead(
      RemoteScriptExitRead.Failed(
        Diagnostics.one(Diagnostic("read-failed", "could not read the exit file")),
        EvidenceBundle(smallEvidence),
        Instant.EPOCH
      )
    )

    accepts("decodeEvidence", AgentDomainJson.decodeEvidence(agentReal))
    accepts(
      "decodeRemoteScriptExitRead",
      AgentDomainJson.decodeRemoteScriptExitRead(remoteReal)
    )

    rejects("decodeEvidence", AgentDomainJson.decodeEvidence(substituteBytes(agentReal, oversize)))
    rejects(
      "decodeRemoteScriptExitRead",
      AgentDomainJson.decodeRemoteScriptExitRead(substituteBytes(remoteReal, oversize))
    )
  }

  private def resultReadsRequest(entries: Int): Json =
    val reference = Json.obj(
      "attemptId" -> Json.fromString("attempt-1"),
      "attemptEpoch" -> Json.fromLong(1L)
    )
    Json.obj(
      "wireVersion" -> Json.fromInt(1),
      "resultRefs" -> Json.fromValues(Vector.fill(entries)(reference)),
      "maximumBytes" -> Json.fromInt(1024)
    )

  test("a result-reference list is capped by cardinality, not only by element size") {
    accepts(
      "decodeRemoteResultReadsRequest",
      AgentDomainJson.decodeRemoteResultReadsRequest(
        resultReadsRequest(RemoteTaskWireLimits.MaximumBatchEntries)
      )
    )
    rejects(
      "decodeRemoteResultReadsRequest",
      AgentDomainJson.decodeRemoteResultReadsRequest(
        resultReadsRequest(RemoteTaskWireLimits.MaximumBatchEntries + 1)
      )
    )
  }

  /** A bound that rejects what a well-behaved peer sends is an outage, not a defence. */
  property("evidence within the limit still decodes") {
    forAll(evidenceBundle) { value =>
      val encoded = AgentDomainJson.encodeEvidence(value)
      assert(
        AgentDomainJson.decodeEvidence(encoded).isRight,
        s"a bound rejected legitimate evidence: ${encoded.noSpaces.take(200)}"
      )
    }
  }

  property("a log read result within the limit still decodes") {
    forAll(logReadResult) { value =>
      val encoded = AgentDomainJson.encodeLogResult(value)
      assert(
        AgentDomainJson.decodeLogResult(encoded).isRight,
        s"a bound rejected a legitimate log result: ${encoded.noSpaces.take(200)}"
      )
    }
  }

  /** Guards the substitution helper itself: if it silently matched nothing, the oversize fixtures
    * would be identical to the accepted ones and every rejection assertion would be vacuous.
    */
  test("the substitution actually replaces the bytes field") {
    val real = AgentDomainJson.encodeEvidence(EvidenceBundle(smallEvidence))
    val substituted = substituteBytes(real, "QUJD")
    assertNotEquals(
      substituted.noSpaces,
      real.noSpaces,
      "substituteBytes matched no field, so the bounds fixtures test nothing"
    )
    assert(
      substituted.noSpaces.contains("QUJD"),
      "substituteBytes did not install the replacement payload"
    )
  }
