package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import scodec.bits.ByteVector

class FrameCodecSuite extends munit.FunSuite:
  private val limits = FrameLimits(ByteLimit.from(32).toOption.get)

  test("decoder handles every possible split of a frame") {
    val payload = ByteVector.view("bounded-frame".getBytes("UTF-8"))
    val encoded = FrameCodec.encode(payload, limits).toOption.get

    (0L to encoded.size).foreach { split =>
      val first = FrameDecoder.empty(limits).feed(encoded.take(split)).toOption.get
      val second = first._1.feed(encoded.drop(split)).toOption.get
      assertEquals(first._2 ++ second._2, Vector(payload), clues(split))
      assertEquals(second._1.finish, Right(()), clues(split))
    }
  }

  test("decoder handles coalesced frames") {
    val one = FrameCodec.encode(ByteVector(1, 2, 3), limits).toOption.get
    val two = FrameCodec.encode(ByteVector(4, 5), limits).toOption.get
    val result = FrameDecoder.empty(limits).feed(one ++ two).toOption.get

    assertEquals(
      result._2,
      Vector(ByteVector(1, 2, 3), ByteVector(4, 5))
    )
    assertEquals(result._1.finish, Right(()))
  }

  test("oversized length is rejected before buffering the payload") {
    val header = ByteVector(0, 0, 1, 0)
    assertEquals(
      FrameDecoder.empty(limits).feed(header),
      Left(FrameFailure.FrameTooLarge(256L, 32))
    )
  }

  test("zero and truncated frames are distinct failures") {
    assertEquals(
      FrameDecoder.empty(limits).feed(ByteVector.fill(4L)(0.toByte)),
      Left(FrameFailure.InvalidLength(0))
    )

    val partial = FrameDecoder.empty(limits).feed(ByteVector(0, 0, 0, 3, 1)).toOption.get
    assertEquals(partial._1.finish, Left(FrameFailure.TruncatedFrame(1L, Some(3))))
  }
