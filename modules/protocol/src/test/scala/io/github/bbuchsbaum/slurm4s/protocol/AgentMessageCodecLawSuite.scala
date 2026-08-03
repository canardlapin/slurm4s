package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.circe.JsonObject
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.ProtocolVersion
import io.github.bbuchsbaum.slurm4s.core.SchemaId
import io.github.bbuchsbaum.slurm4s.core.codec.CodecFailure
import io.github.bbuchsbaum.slurm4s.core.codec.VersionedJson
import io.github.bbuchsbaum.slurm4s.core.codec.WireEnvelope

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import scodec.bits.ByteVector

import scala.util.Try

/** Envelope laws for the agent message codec, framing included (P6f.34).
  *
  * The envelope is what every agent exchange travels inside, so its laws are stated over the whole
  * transport rather than the JSON alone: a message is encoded, framed, cut into arbitrary chunks,
  * reassembled and decoded. Testing the JSON layer by itself would leave the seam between it and
  * the framing untested, and that seam is where a message either survives a pipe or does not.
  */
class AgentMessageCodecLawSuite extends munit.ScalaCheckSuite:
  import ProtocolGenerators.*

  // Neither type derives CanEqual; supplied here rather than on the public types, since whether the
  // transport envelope should carry structural equality belongs to the codec-ownership work.
  given bodyCanEqual: CanEqual[AgentBody, AgentBody] = CanEqual.derived
  given envelopeCanEqual: CanEqual[AgentEnvelope, AgentEnvelope] = CanEqual.derived

  private val limits = FrameLimits(ByteLimit.maximumCommandCapture)

  property("an agent message round-trips through its encoding") {
    forAll(agentEnvelope) { message =>
      assertEquals(AgentMessageCodec.decode(AgentMessageCodec.encode(message)), Right(message))
    }
  }

  /** The composed law, and the one the transport actually depends on: encode, frame, cut the stream
    * wherever, reassemble, decode. Every layer between the caller and the pipe is in scope.
    */
  property("an agent message survives framing and any chunking of the stream") {
    val scenario = agentEnvelope.flatMap { message =>
      val framed = FrameCodec.encode(AgentMessageCodec.encode(message), limits).toOption.get
      chunkings(framed).map((message, _))
    }
    forAll(scenario) { (message, chunks) =>
      val recovered = chunks.foldLeft((FrameDecoder.empty(limits), Vector.empty[ByteVector])) {
        case ((decoder, frames), chunk) =>
          val (next, decoded) = decoder.feed(chunk).toOption.get
          (next, frames ++ decoded)
      }
      assertEquals(recovered._1.finish, Right(()), "the framed message did not end cleanly")
      assertEquals(
        recovered._2.map(AgentMessageCodec.decode),
        Vector(Right(message)),
        s"a message cut into ${chunks.size} chunks did not survive the round trip"
      )
      true
    }
  }

  /** Several messages pipelined into one stream, which is what a client that does not wait for each
    * response produces. Order is part of the contract: responses are matched to requests by id, but
    * a transport that reordered frames would break every caller that assumed otherwise.
    */
  property("pipelined messages arrive in the order they were written") {
    val scenario = Gen.choose(1, 5).flatMap(Gen.listOfN(_, agentEnvelope)).flatMap { messages =>
      val stream = messages.foldLeft(ByteVector.empty) { (accumulated, message) =>
        accumulated ++ FrameCodec.encode(AgentMessageCodec.encode(message), limits).toOption.get
      }
      chunkings(stream).map((messages.toVector, _))
    }
    forAll(scenario) { (messages, chunks) =>
      val recovered = chunks.foldLeft((FrameDecoder.empty(limits), Vector.empty[ByteVector])) {
        case ((decoder, frames), chunk) =>
          val (next, decoded) = decoder.feed(chunk).toOption.get
          (next, frames ++ decoded)
      }
      assertEquals(recovered._2.map(AgentMessageCodec.decode), messages.map(Right(_)))
    }
  }

  /** Forward compatibility, which this codec implements unusually: an unrecognized field is not
    * merely tolerated, it is surfaced as an extension. That is the mechanism additive protocol
    * changes ride on, so the law is preservation rather than indifference — a decoder that dropped
    * the field would satisfy "does not reject" while quietly discarding what a newer peer sent.
    */
  property("a field written by a newer peer is preserved as an extension") {
    val scenario = Gen.zip(
      agentEnvelope,
      Gen.oneOf("fieldFromANewerPeer", "traceparent", "budgetRemaining"),
      JsonCorpus.arbitraryJson
    )
    forAll(scenario) { (message, name, value) =>
      // Skip the draws where the generator already used this name; adding it would not be new.
      if message.extensions.contains(name) then true
      else
        val extended = injectIntoPayload(AgentMessageCodec.encode(message), name, value)
        AgentMessageCodec.decode(extended) match
          case Left(failure)  => fail(s"an added field was rejected: $failure")
          case Right(decoded) =>
            assertEquals(
              decoded.extensions(name),
              Some(value),
              s"the added field $name was not preserved as an extension"
            )
            assertEquals(decoded.body, message.body, "the added field disturbed the body")
            assertEquals(decoded.requestId, message.requestId)
            true
    }
  }

  /** Rewrites the encoded envelope's payload object to carry an extra field, as a newer peer would.
    */
  private def injectIntoPayload(bytes: ByteVector, name: String, value: Json): ByteVector =
    val envelope = VersionedJson.decode(bytes).toOption.get
    val payload = envelope.payload.asObject.getOrElse(JsonObject.empty).add(name, value)
    VersionedJson.encode(
      WireEnvelope(envelope.protocol, envelope.schema, Json.fromJsonObject(payload))
    )

  /** A message from a different schema must be refused by name rather than misread. The envelope
    * carries the schema precisely so that two protocols sharing a transport cannot be confused, and
    * a decoder that ignored it would happily interpret another schema's fields as its own.
    */
  property("a message carrying another schema is refused by name") {
    val scenario = Gen.zip(
      agentEnvelope,
      Gen.oneOf("slurm4s.control-command", "slurm4s.result-envelope", "other.schema")
    )
    forAll(scenario) { (message, schema) =>
      val original = VersionedJson.decode(AgentMessageCodec.encode(message)).toOption.get
      val relabelled = VersionedJson.encode(
        WireEnvelope(original.protocol, SchemaId.unsafeFrom(schema), original.payload)
      )
      assertEquals(
        AgentMessageCodec.decode(relabelled),
        Left(AgentCodecFailure.WrongSchema(schema))
      )
    }
  }

  /** An unsupported major must be refused as a version failure, not as malformed JSON: the caller
    * has to be able to tell "I cannot speak to this peer" from "this peer sent me garbage".
    */
  property("an unsupported protocol major is refused as a version failure") {
    forAll(Gen.oneOf(0, 2, 3, 99).suchThat(_ != VersionedJson.supportedMajor)) { major =>
      val message = agentEnvelope.sample.getOrElse(fail("no message was generated"))
      val text = AgentMessageCodec
        .encode(message)
        .decodeUtf8
        .toOption
        .get
        .replaceFirst("\"major\":1", s"\"major\":$major")
      AgentMessageCodec.decode(ByteVector.view(text.getBytes("UTF-8"))) match
        case Left(AgentCodecFailure.Envelope(CodecFailure.UnsupportedMajor(received, supported))) =>
          assertEquals(received, major)
          assertEquals(supported, VersionedJson.supportedMajor)
          true
        case other => fail(s"a major of $major was not refused as a version failure: $other")
    }
  }

  private def neverThrows(bytes: ByteVector)(using munit.Location): Boolean =
    Try(AgentMessageCodec.decode(bytes)) match
      case scala.util.Success(_)     => true
      case scala.util.Failure(error) =>
        fail(
          s"decode threw ${error.getClass.getName} instead of returning a Left for " +
            bytes.take(120L).toHex
        )

  /** Arbitrary bytes, not merely arbitrary JSON. A frame payload is whatever the peer put in it, so
    * invalid UTF-8 reaches this decoder and has to come back as a failure value like anything else.
    */
  property("the message decoder never throws on arbitrary bytes") {
    val bytes = Gen.oneOf(
      Gen.choose(0, 64).flatMap(Gen.listOfN(_, Gen.choose(Byte.MinValue, Byte.MaxValue))).map {
        values => ByteVector(values*)
      },
      // Lone continuation bytes and a truncated multi-byte sequence: invalid UTF-8 by construction.
      Gen.oneOf(
        ByteVector(0x80.toByte),
        ByteVector(0xc3.toByte),
        ByteVector(0xff.toByte, 0xfe.toByte),
        ByteVector(0xed.toByte, 0xa0.toByte, 0x80.toByte)
      ),
      JsonCorpus.arbitraryJson.map(json => ByteVector.view(json.noSpaces.getBytes("UTF-8")))
    )
    forAll(bytes)(neverThrows)
  }

  property("the message decoder never throws on a corrupted valid message") {
    val corrupted = agentEnvelope
      .map(message => VersionedJson.decode(AgentMessageCodec.encode(message)).toOption.get)
      .flatMap(envelope =>
        JsonCorpus
          .mutations(envelope.payload)
          .map(payload =>
            VersionedJson.encode(WireEnvelope(envelope.protocol, envelope.schema, payload))
          )
      )
    forAll(corrupted)(neverThrows)
  }

  /** Canonical encoding, which the durable journal's checksum depends on and which makes any
    * byte-level comparison of two messages meaningful in the first place.
    */
  property("encoding a message twice produces identical bytes") {
    forAll(agentEnvelope) { message =>
      assertEquals(AgentMessageCodec.encode(message), AgentMessageCodec.encode(message))
    }
  }

  /** Extension names are refused for exactly the reserved set, no wider and no narrower. Too wide
    * and additive protocol changes lose usable names; too narrow and an extension could overwrite
    * the field carrying the request id or the body.
    */
  test("construction refuses exactly the reserved extension names") {
    val reserved = Vector("requestId", "kind", "method", "status", "body")
    val id = requestId.sample.getOrElse(fail("no request id was generated"))
    val body = agentBody.sample.getOrElse(fail("no body was generated"))

    reserved.foreach { name =>
      val extensions = JsonObject.singleton(name, Json.fromString("hijacked"))
      assertEquals(
        AgentEnvelope.withExtensions(id, ProtocolVersion.v1, body, extensions),
        Left(AgentExtensionFailure(name)),
        s"$name was accepted as an extension even though the encoding uses it"
      )
    }

    Vector("requestIdentifier", "kinds", "bodyText", "trace").foreach { name =>
      val extensions = JsonObject.singleton(name, Json.fromString("fine"))
      assert(
        AgentEnvelope.withExtensions(id, ProtocolVersion.v1, body, extensions).isRight,
        s"$name was refused even though nothing in the encoding uses it"
      )
    }
  }

  /** Guards the round-trip law against being tested only on messages with nothing to preserve. */
  test("the message generator reaches extensions, and both body shapes") {
    val messages = Vector.fill(200)(agentEnvelope.sample).flatten
    assert(messages.sizeIs > 0, "no messages were generated")
    assert(
      messages.exists(_.extensions.nonEmpty),
      "every generated message had empty extensions, so the round-trip law never preserved one"
    )
    assert(
      messages.exists(_.body match
        case AgentBody.Request(_, _) => true
        case _                       => false),
      "no request bodies were generated"
    )
    assert(
      messages.exists(_.body match
        case AgentBody.Response(_, _) => true
        case _                        => false),
      "no response bodies were generated"
    )
  }
