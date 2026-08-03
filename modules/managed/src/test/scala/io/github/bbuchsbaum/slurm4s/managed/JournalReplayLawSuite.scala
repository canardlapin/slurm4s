package io.github.bbuchsbaum.slurm4s.managed

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.codec.CanonicalJson
import io.github.bbuchsbaum.slurm4s.protocol.AgentDomainJson

import java.nio.file.Files
import java.nio.file.Path

/** Codec laws for the on-disk control journal (P6f.34).
  *
  * `JournalCodec` is private to its file, and deliberately so: the framing, the schema envelope and
  * the SHA-256 over the canonically printed command are one unit, and a caller that could reach
  * half of them could write a record the reader would reject. So these laws drive the real store
  * instead of the codec, which also makes them stronger — they cover framing, canonicalization,
  * checksum and command codec together, in the arrangement production uses.
  *
  * The law itself: replay reproduces the live state exactly. That is the property the durability
  * claim rests on, since a restarted controller decides what work remains from the replayed state
  * alone. It is an exact comparison because the live commit path and the replay path apply the same
  * transition and compact the event cache identically, so any difference is a codec defect rather
  * than an expected divergence.
  */
class JournalReplayLawSuite extends munit.CatsEffectSuite:
  import ManagedGenerators.*

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("slurm4s-journal-law")))(directory =>
      IO.blocking {
        Files.walk(directory).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
      }
    )

  /** Applies a scenario to a fresh journal, then reopens it and returns both states plus how many
    * commands the transition rules actually accepted.
    */
  private def liveAndReplayed(
      commands: Vector[ControlCommand]
  ): IO[(ControlState, ControlState, Int)] =
    temporaryDirectory.use { directory =>
      val path = directory.resolve("control.journal")
      for
        live <- FileJournalControlStore.open[IO](path).use { store =>
          commands
            .traverse(store.transact)
            .flatMap(outcomes => store.snapshot.map(state => (state, outcomes.count(_.isRight))))
        }
        replayed <- FileJournalControlStore.open[IO](path).use(_.snapshot)
      yield (live._1, replayed, live._2)
    }

  test("a journal replays to exactly the state that wrote it") {
    val scenarios = Vector.fill(24)(controlScenario.sample).flatten
    assert(scenarios.sizeIs > 0, "no scenarios were generated")
    scenarios
      .traverse { commands =>
        liveAndReplayed(commands).map { (live, replayed, accepted) =>
          assertEquals(
            replayed,
            live,
            s"replaying ${commands.size} commands did not reproduce the state that wrote them"
          )
          (live, accepted)
        }
      }
      .map { outcomes =>
        // Without this the law could hold over journals holding one or two records, which would say
        // nothing about the commands whose codecs carry the most fields.
        val deepest = outcomes.map(_._2).max
        assert(
          deepest >= 4,
          s"the deepest scenario committed only $deepest commands, so the replay law never " +
            "reconstructed a state built from more than a claim"
        )
        assert(
          outcomes.forall((live, _) => live.attempts.nonEmpty && live.events.nonEmpty),
          "a scenario produced no attempt or no event, so its journal was effectively empty"
        )
      }
  }

  /** A corrupted record must be reported as a typed failure, not replayed as a different command.
    *
    * The checksum over the canonically printed command is what makes this detectable: without it, a
    * flipped byte inside the command body could still parse as valid JSON and replay as a command
    * the controller never issued.
    */
  test("a record whose command body was altered is refused rather than replayed") {
    val scenario = controlScenario.sample.getOrElse(fail("no scenario was generated"))
    temporaryDirectory.use { directory =>
      val path = directory.resolve("control.journal")
      for
        _ <- FileJournalControlStore.open[IO](path).use(store => scenario.traverse_(store.transact))
        original <- IO.blocking(Files.readAllBytes(path))
        _ <- IO.blocking(Files.write(path, corruptCommandBody(original)))
        reopened <- FileJournalControlStore.open[IO](path).use(_.snapshot).attempt
      yield reopened match
        case Left(_)      => ()
        case Right(state) =>
          fail(
            "a journal with an altered command body replayed cleanly to " +
              s"${state.attempts.size} attempts, so the checksum is not protecting the body"
          )
    }
  }

  /** Rewrites a digit inside the record's JSON without changing its length, so framing still lines
    * up and the checksum is the only thing that can notice.
    */
  private def corruptCommandBody(bytes: Array[Byte]): Array[Byte] =
    val text = new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1)
    val marker = "\"epoch\":"
    val at = text.indexOf(marker)
    assert(at >= 0, "the journal did not contain an epoch field to alter")
    val target = at + marker.length
    val digit = text.charAt(target)
    assert(digit.isDigit, s"expected a digit after $marker but found '$digit'")
    val altered = if digit == '9' then '1' else (digit + 1).toChar
    val copy = bytes.clone()
    copy(target) = altered.toByte
    copy

  /** Canonicalization, which the checksum depends on: the printer sorts keys, so encoding a command
    * twice must produce byte-identical output. If it did not, a record's checksum could disagree
    * with a re-encoding of the same command and a valid journal would fail to replay.
    */
  test("encoding the same command twice produces identical bytes") {
    val commands = Vector.fill(200)(controlCommand.sample).flatten
    assert(commands.sizeIs > 0, "no commands were generated")
    commands.foreach { command =>
      val first = ControlCommandJson.encode(command).noSpaces
      val second = ControlCommandJson.encode(command).noSpaces
      assertEquals(second, first, s"encoding was not deterministic for $command")
    }
  }

  /** Pins the other durable consumer of canonicalization to the one shared rendering (P7.4).
    *
    * A managed request's digest of its canonical bytes *is* its identity — the controller treats
    * two requests with the same digest as the same request — so if this object went back to
    * rendering with a printer of its own, an identical request could canonicalize differently and
    * stop matching what was already recorded. Comparing against `CanonicalJson` is what makes that
    * regression fail a test rather than surface as a phantom digest conflict.
    */
  test("a canonical request is rendered by the shared canonicalization, not a private printer") {
    val specs = Vector.fill(50)(ManagedGenerators.managedLaunchSpec.sample).flatten
    assert(specs.sizeIs > 0, "no requests were generated")
    specs.foreach { spec =>
      val canonical = CanonicalRequest
        .from(spec)
        .fold(failure => fail(s"a generated request did not canonicalize: $failure"), identity)
      val expected = CanonicalJson.bytes(
        AgentDomainJson.encodeSubmitRequest(spec).toOption.get
      )
      assertEquals(
        canonical.bytes,
        expected,
        "the canonical request bytes diverged from the shared canonical rendering"
      )
    }
  }

  /** Guards the corruption test against depending on a specific scenario: the marker it rewrites
    * has to be present in the journals these scenarios write.
    */
  test("the generated scenarios write a record the corruption test can reach") {
    val scenarios = Vector.fill(8)(controlScenario.sample).flatten
    val encoded = scenarios.flatMap(_.map(ControlCommandJson.encode(_).noSpaces))
    assert(
      encoded.exists(_.contains("\"epoch\":")),
      "no generated command carried an epoch, so the corruption test has nothing to alter"
    )
  }
