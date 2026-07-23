package io.github.bbuchsbaum.scalaslurm.protocol

import io.github.bbuchsbaum.scalaslurm.core.ByteLimit

class FrameCodecSuite extends munit.FunSuite:
  private val limits = FrameLimits(ByteLimit.from(32).toOption.get)

  test("decoder handles every possible split of a frame") {
    val payload = "bounded-frame".getBytes("UTF-8").toVector
    val encoded = FrameCodec.encode(payload, limits).toOption.get

    (0 to encoded.size).foreach { split =>
      val first = FrameDecoder.empty(limits).feed(encoded.take(split)).toOption.get
      val second = first._1.feed(encoded.drop(split)).toOption.get
      assertEquals(first._2 ++ second._2, Vector(payload), clues(split))
      assertEquals(second._1.finish, Right(()), clues(split))
    }
  }

  test("decoder handles coalesced frames") {
    val one = FrameCodec.encode(Vector(1, 2, 3), limits).toOption.get
    val two = FrameCodec.encode(Vector(4, 5), limits).toOption.get
    val result = FrameDecoder.empty(limits).feed(one ++ two).toOption.get

    assertEquals(
      result._2,
      Vector(Vector[Byte](1, 2, 3), Vector[Byte](4, 5))
    )
    assertEquals(result._1.finish, Right(()))
  }

  test("oversized length is rejected before buffering the payload") {
    val header = Vector(0.toByte, 0.toByte, 1.toByte, 0.toByte)
    assertEquals(
      FrameDecoder.empty(limits).feed(header),
      Left(FrameFailure.FrameTooLarge(256, 32))
    )
  }

  test("zero and truncated frames are distinct failures") {
    assertEquals(
      FrameDecoder.empty(limits).feed(Vector.fill(4)(0.toByte)),
      Left(FrameFailure.InvalidLength(0))
    )

    val partial = FrameDecoder.empty(limits).feed(Vector(0, 0, 0, 3, 1).map(_.toByte)).toOption.get
    assertEquals(partial._1.finish, Left(FrameFailure.TruncatedFrame(1, Some(3))))
  }
