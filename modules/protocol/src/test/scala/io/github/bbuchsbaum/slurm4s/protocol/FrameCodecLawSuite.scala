package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.ByteLimit

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import scodec.bits.ByteVector

/** Framing laws for the length-prefixed stdio transport (P6f.34).
  *
  * The framing layer is the one place in the protocol that cannot assume its input is well-formed
  * *or* well-aligned. It reads from a pipe, so a chunk boundary has nothing to do with a frame
  * boundary: a single read can deliver half a header, three whole frames and the first byte of a
  * fourth. A decoder tested only on whole frames has never been tested against the input it will
  * actually get.
  *
  * The governing law is therefore chunk independence — the frames recovered from a stream must not
  * depend on how that stream was cut up. Everything else here is a bound or a refusal.
  */
class FrameCodecLawSuite extends munit.ScalaCheckSuite:
  import ProtocolGenerators.chunkings

  private val limits = FrameLimits(ByteLimit.unsafeFrom(4096))

  private val payload: Gen[ByteVector] =
    Gen
      .choose(1, 300)
      .flatMap(Gen.listOfN(_, Gen.choose(Byte.MinValue, Byte.MaxValue)))
      .map(bytes => ByteVector(bytes*))

  private val payloads: Gen[Vector[ByteVector]] =
    Gen.choose(1, 6).flatMap(Gen.listOfN(_, payload)).map(_.toVector)

  private def stream(values: Vector[ByteVector]): ByteVector =
    values.foldLeft(ByteVector.empty)((accumulated, value) =>
      accumulated ++ FrameCodec.encode(value, limits).toOption.get
    )

  /** Feeds a chunked stream and returns the frames recovered, or the failure that stopped it. */
  private def feedAll(
      chunks: Vector[ByteVector]
  ): Either[FrameFailure, (FrameDecoder, Vector[ByteVector])] =
    chunks.foldLeft[Either[FrameFailure, (FrameDecoder, Vector[ByteVector])]](
      Right((FrameDecoder.empty(limits), Vector.empty))
    ) { (state, chunk) =>
      state.flatMap { (decoder, recovered) =>
        decoder.feed(chunk).map((next, frames) => (next, recovered ++ frames))
      }
    }

  property("frames survive any chunking of the stream that carried them") {
    val scenario = payloads.flatMap(values => chunkings(stream(values)).map((values, _)))
    forAll(scenario) { (values, chunks) =>
      feedAll(chunks) match
        case Left(failure)               => fail(s"a well-formed stream failed to decode: $failure")
        case Right((decoder, recovered)) =>
          assertEquals(
            recovered,
            values,
            s"a stream of ${values.size} frames cut into ${chunks.size} chunks decoded wrongly"
          )
          assertEquals(decoder.finish, Right(()), "the decoder did not end on a frame boundary")
          true
    }
  }

  /** The same claim stated as a difference, which is what makes a chunk-sensitive bug obvious: two
    * cuttings of one stream must agree, so a decoder that mishandled a split would disagree with
    * itself rather than merely fail an expected value.
    */
  property("two different chunkings of one stream recover the same frames") {
    val scenario = payloads.flatMap { values =>
      val bytes = stream(values)
      Gen.zip(chunkings(bytes), chunkings(bytes))
    }
    forAll(scenario) { (left, right) =>
      assertEquals(feedAll(right).map(_._2), feedAll(left).map(_._2))
    }
  }

  /** Every prefix of a stream is a legal intermediate state: it must either be refused or leave the
    * decoder waiting, and it must never invent a frame that the full stream would not produce.
    */
  property("a truncated stream yields a prefix of the frames and never more") {
    val scenario = payloads.flatMap { values =>
      val bytes = stream(values)
      Gen.choose(0L, bytes.size).map(cut => (values, bytes.take(cut)))
    }
    forAll(scenario) { (values, truncated) =>
      feedAll(Vector(truncated)) match
        case Left(failure) => fail(s"a prefix of a valid stream was refused: $failure")
        case Right((decoder, recovered)) =>
          assert(
            values.startsWith(recovered),
            s"a truncated stream produced ${recovered.size} frames that are not a prefix " +
              s"of the ${values.size} frames it was cut from"
          )
          // finish must agree with whether the cut landed on a boundary, and the only way it can be
          // on a boundary is if every frame arrived.
          assertEquals(
            decoder.finish.isRight,
            recovered.size == values.size && truncated.size == stream(values).size,
            "finish disagreed with whether the stream ended on a frame boundary"
          )
          true
    }
  }

  property("a payload at the size limit is framed and a payload past it is refused") {
    val atLimit = ByteVector.fill(limits.maximumPayloadBytes.value.toLong)(7.toByte)
    assertEquals(
      FrameCodec.encode(atLimit, limits).map(_.size),
      Right(atLimit.size + FrameCodec.HeaderBytes.toLong)
    )
    assertEquals(
      FrameCodec.encode(atLimit :+ 7.toByte, limits),
      Left(FrameFailure.FrameTooLarge(atLimit.size + 1L, limits.maximumPayloadBytes.value))
    )
    assertEquals(FrameCodec.encode(ByteVector.empty, limits), Left(FrameFailure.EmptyPayload))
  }

  /** Verifies the claim `FrameDecoder.readLength` documents but nothing tested: a header whose top
    * bit is set arrives as a negative int, and must be refused as an invalid length rather than
    * sign-extended into a huge allocation or silently believed.
    */
  property("a header claiming two gibibytes or more is refused as an invalid length") {
    val highBitHeaders = Gen.oneOf(
      ByteVector(0x80.toByte, 0, 0, 0),
      ByteVector(0xff.toByte, 0xff.toByte, 0xff.toByte, 0xff.toByte),
      ByteVector(0xc0.toByte, 0, 0, 1)
    )
    forAll(highBitHeaders) { header =>
      FrameDecoder.empty(limits).feed(header) match
        case Left(FrameFailure.InvalidLength(received)) =>
          assert(received < 0, s"expected a negative length but the decoder read $received")
          true
        case other =>
          fail(s"a header with its top bit set was not refused as an invalid length: $other")
    }
  }

  /** A header inside the signed range but past the negotiated limit must be refused from the header
    * alone, before the payload it claims is buffered. Otherwise the limit is not a defence: a peer
    * could make the reader accumulate as much as it liked simply by never sending the body.
    */
  property("an oversize header is refused without buffering the payload it claims") {
    val oversize = Gen.choose(limits.maximumPayloadBytes.value + 1, Int.MaxValue)
    forAll(oversize) { length =>
      val header = ByteVector.fromInt(length, size = FrameCodec.HeaderBytes)
      assertEquals(
        FrameDecoder.empty(limits).feed(header),
        Left(FrameFailure.FrameTooLarge(length.toLong, limits.maximumPayloadBytes.value))
      )
    }
  }

  /** Proves the chunk-independence law is sensitive rather than merely green.
    *
    * This is the implementation the law exists to rule out: a reader that treats each chunk as a
    * whole frame, which is exactly what you write if you test only on whole frames. The law must
    * disagree with it, otherwise it would pass against a transport that corrupted every split
    * message.
    */
  test("a reader that assumes frames align with chunks is caught by the chunking law") {
    def naive(chunks: Vector[ByteVector]): Vector[String] =
      chunks
        .filter(_.size > FrameCodec.HeaderBytes.toLong)
        .map(_.drop(FrameCodec.HeaderBytes.toLong).toHex)

    val values = payloads.sample.getOrElse(fail("no payloads were generated"))
    val expected = values.map(_.toHex)
    val bytes = stream(values)
    val samples = Vector.fill(200)(chunkings(bytes).sample).flatten
    assert(samples.sizeIs > 0, "no chunkings were generated")

    assert(
      samples.exists(chunks => naive(chunks) != expected),
      "the chunk-aligned reader agreed with every generated chunking, so the law would not " +
        "detect a decoder that ignored frame boundaries"
    )
    // And the real decoder agrees on all of them, which is the contrast that makes the law meaningful.
    samples.foreach(chunks => assertEquals(feedAll(chunks).map(_._2), Right(values)))
  }

  /** Guards the chunking laws against testing only the easy shape. If every generated chunking
    * happened to align with frame boundaries, the laws above would pass without ever exercising a
    * split header, which is the case that matters.
    */
  test("the generated chunkings actually split frames and headers") {
    val values = payloads.sample.getOrElse(fail("no payloads were generated"))
    val bytes = stream(values)
    val samples = Vector.fill(200)(chunkings(bytes).sample).flatten
    assert(samples.sizeIs > 0, "no chunkings were generated")

    val boundaries = values
      .scanLeft(0L)((at, value) => at + FrameCodec.HeaderBytes.toLong + value.size)
      .toSet
    val cuts = samples.map(_.scanLeft(0L)((at, chunk) => at + chunk.size).toSet)
    assert(
      cuts.exists(_.exists(cut => !boundaries.contains(cut))),
      "every generated chunking fell on a frame boundary, so no split frame was ever decoded"
    )
    assert(
      cuts.exists(_.exists(cut => cut > 0L && cut < FrameCodec.HeaderBytes.toLong)),
      "no generated chunking split the first header, so partial-header handling is untested"
    )
  }
