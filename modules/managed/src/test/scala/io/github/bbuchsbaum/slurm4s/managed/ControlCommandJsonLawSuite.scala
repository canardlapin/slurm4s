package io.github.bbuchsbaum.slurm4s.managed

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.JsonCorpus

import org.scalacheck.Prop.forAll

import scala.util.Try

/** Codec laws for the persisted control command (P6f.34).
  *
  * These commands are the write-ahead record a restarted controller replays to learn what it
  * already did. That makes the stakes different from the wire codecs: a wire message that decodes
  * wrongly fails one exchange, while a journal record that decodes wrongly makes the controller act
  * on a history that never happened — resubmitting a job it already submitted, or forgetting a
  * cancellation it already claimed.
  *
  * Three laws, matching the wire suites: every command round-trips exactly, no input makes the
  * decoder throw, and an unknown field from a newer writer is ignored rather than rejected.
  */
class ControlCommandJsonLawSuite extends munit.ScalaCheckSuite:
  import ManagedGenerators.*
  import JsonCorpus.*

  // ControlCommand does not `derive CanEqual`. Supplied here rather than on the type, because
  // whether the public enum should carry structural equality belongs to the codec-ownership work.
  given controlCommandCanEqual: CanEqual[ControlCommand, ControlCommand] = CanEqual.derived

  property("every control command round-trips through the journal encoding") {
    forAll(controlCommand) { command =>
      val encoded = ControlCommandJson.encode(command)
      ControlCommandJson.decode(encoded) match
        case Right(decoded) if decoded == command => true
        case Right(decoded)                       =>
          fail(
            s"a round-trip changed the command\n  before: $command\n" +
              s"  after:  $decoded\n  encoded as: ${encoded.noSpaces}"
          )
        case Left(problem) =>
          fail(s"a valid command failed to decode: $problem\n  encoded as: ${encoded.noSpaces}")
    }
  }

  private def neverThrows(json: Json)(using munit.Location): Boolean =
    Try(ControlCommandJson.decode(json)) match
      case scala.util.Success(_)     => true
      case scala.util.Failure(error) =>
        fail(
          s"decode threw ${error.getClass.getName} instead of returning a Left" +
            s" for ${json.noSpaces.take(300)}"
        )

  property("the command decoder never throws on arbitrary JSON") {
    forAll(arbitraryJson)(neverThrows)
  }

  /** The important half: a corrupted-but-plausible record is what a torn or partially-rewritten
    * journal actually looks like, and it gets past the discriminator into the field reads.
    */
  property("the command decoder never throws on a corrupted command record") {
    val corrupted = controlCommand.map(ControlCommandJson.encode).flatMap(mutations)
    forAll(corrupted)(neverThrows)
  }

  property("the command decoder never throws when a nested field is corrupted") {
    val corrupted = controlCommand.map(ControlCommandJson.encode).flatMap(nestedMutations)
    forAll(corrupted)(neverThrows)
  }

  /** Forward compatibility for the journal, which is the case that matters most: a journal written
    * by a newer build must still replay on an older one, or a rollback cannot read its own history.
    */
  property("a field written by a newer build is ignored, not rejected") {
    forAll(controlCommand) { command =>
      val encoded = ControlCommandJson.encode(command)
      withUnknownField(encoded).sample.forall { extended =>
        ControlCommandJson.decode(extended) match
          case Right(decoded) if decoded == command => true
          case other                                =>
            fail(
              s"an added field changed the replayed command\n  expected: Right($command)\n" +
                s"  obtained: $other"
            )
      }
    }
  }

  /** Guards the properties above against passing vacuously: if every generated record were rejected
    * at the discriminator, "never throws" would hold while saying nothing about the field reads.
    */
  test("the command corpora reach full decodes as well as discriminator rejections") {
    val valid = Vector
      .fill(200)(controlCommand.sample)
      .flatten
      .map(command => (command, ControlCommandJson.encode(command)))
    assert(valid.sizeIs > 0, "no commands were generated")

    val extended = valid.flatMap((command, json) => withUnknownField(json).sample.map((command, _)))
    assert(
      extended.exists((command, json) => ControlCommandJson.decode(json) == Right(command)),
      "no unknown-field record decoded successfully, so the corpus never exercises a full decode"
    )

    val corrupted = valid.flatMap((_, json) => mutations(json).sample)
    assert(
      corrupted.exists(json => ControlCommandJson.decode(json).isLeft),
      "no corrupted record was rejected, so the mutations are not corrupting anything"
    )
  }

  /** The generator-strength counterpart of the round-trip law.
    *
    * `ManagedIntent.retrySafety` defaults to `Unknown` and the encoder omits it at that value, so a
    * codec that never wrote the field would still round-trip every intent a lazy generator
    * produced. This asserts the generator actually reaches the other values, which is what makes
    * the round-trip law above load-bearing for this field.
    */
  test("the intent generator reaches a retry safety the encoder does not omit") {
    val intents = Vector.fill(200)(managedIntent.sample).flatten
    assert(intents.sizeIs > 0, "no intents were generated")
    val stated = intents.filter(_.retrySafety != RetrySafety.Unknown)
    assert(
      stated.nonEmpty,
      "every generated intent left retrySafety at Unknown, which the encoder omits, so the " +
        "round-trip law would hold even for a codec that dropped the field"
    )
    stated.foreach { intent =>
      val encoded = ControlCommandJson.encode(ControlCommand.RecordIntent(intent))
      assert(
        encoded.hcursor.downField("retrySafety").succeeded,
        s"a stated retrySafety was not written: ${encoded.noSpaces}"
      )
    }
  }

  /** The epoch is what fences a stale attempt out. A generator pinned to the initial epoch would
    * let a codec that hardcoded it pass, so the round-trip law needs the generator to move it.
    */
  test("the intent generator reaches an epoch past the initial one") {
    val intents = Vector.fill(200)(managedIntent.sample).flatten
    assert(
      intents.exists(_.epoch != AttemptEpoch.initial),
      "every generated intent sat at the initial epoch, so a codec that hardcoded it would pass"
    )
  }

  /** The bounds law. A journal frame may be far larger than a canonical request is allowed to be,
    * so an oversize `requestBase64` has to be refused from its encoded length rather than after the
    * decoder has already allocated it.
    */
  test("an oversize canonical request is refused before it is decoded") {
    val intent = managedIntent.sample.getOrElse(fail("no intent was generated"))
    val encoded = ControlCommandJson.encode(ControlCommand.RecordIntent(intent))
    val obj = encoded.asObject.getOrElse(fail("a recorded intent did not encode as an object"))

    // Base64 of a payload one byte past the limit: valid base64, so only the bound can reject it.
    val oversize = "A" * (((ByteLimit.maximumCommandCapture.value + 1 + 2) / 3) * 4)
    val inflated = Json.fromJsonObject(obj.add("requestBase64", Json.fromString(oversize)))

    ControlCommandJson.decode(inflated) match
      case Left(problem) =>
        assert(
          problem.contains("exceed"),
          s"the oversize request was refused, but for the wrong reason: $problem"
        )
      case Right(_) => fail("an oversize canonical request was accepted")
    assert(
      ControlCommandJson.decode(encoded).isRight,
      "the in-bound fixture this test inflates does not itself decode, so the refusal above " +
        "proves nothing about the bound"
    )
  }

  /** Proves the round-trip law is sensitive rather than merely green.
    *
    * Removing a field from a valid encoding is exactly what a codec that forgot to write it would
    * produce. If the law could not tell the difference, it would pass against that codec too, so
    * each field named here is one the law is demonstrably load-bearing for. `retrySafety` is the
    * sharp case: the encoder omits it at its default, so only a generator that reaches the other
    * values makes its absence observable at all.
    */
  test("removing a field from a valid encoding is detected by the round-trip law") {
    val fields = Vector("retrySafety", "epoch", "submissionKey", "attemptId", "requestDigest")
    val intents = Vector
      .fill(200)(managedIntent.sample)
      .flatten
      .filter(_.retrySafety != RetrySafety.Unknown)
    assert(intents.sizeIs > 0, "no intents with a stated retry safety were generated")

    fields.foreach { field =>
      val survived = intents.filter { intent =>
        val command = ControlCommand.RecordIntent(intent)
        val stripped = ControlCommandJson
          .encode(command)
          .asObject
          .map(obj => Json.fromJsonObject(obj.remove(field)))
          .getOrElse(Json.Null)
        ControlCommandJson.decode(stripped) == Right(command)
      }
      assert(
        survived.isEmpty,
        s"dropping $field still round-tripped, so the law cannot detect a codec that omits it"
      )
    }
  }
