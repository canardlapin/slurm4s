package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.ByteLimit

final case class FrameLimits(maximumPayloadBytes: ByteLimit) derives CanEqual

object FrameLimits:
  val default: FrameLimits = FrameLimits(ByteLimit.maximumCommandCapture)

enum FrameFailure derives CanEqual:
  case EmptyPayload
  case InvalidLength(received: Int)
  case FrameTooLarge(received: Int, maximum: Int)
  case TruncatedFrame(bufferedBytes: Int, expectedPayloadBytes: Option[Int])

object FrameCodec:
  val HeaderBytes: Int = 4

  def encode(payload: Vector[Byte], limits: FrameLimits): Either[FrameFailure, Vector[Byte]] =
    val length = payload.size
    if length == 0 then Left(FrameFailure.EmptyPayload)
    else if length > limits.maximumPayloadBytes.value then
      Left(FrameFailure.FrameTooLarge(length, limits.maximumPayloadBytes.value))
    else
      Right(
        Vector(
          ((length >>> 24) & 0xff).toByte,
          ((length >>> 16) & 0xff).toByte,
          ((length >>> 8) & 0xff).toByte,
          (length & 0xff).toByte
        ) ++ payload
      )

final case class FrameDecoder private (
    limits: FrameLimits,
    buffered: Vector[Byte],
    expectedPayloadBytes: Option[Int]
):
  def feed(input: Vector[Byte]): Either[FrameFailure, (FrameDecoder, Vector[Vector[Byte]])] =
    var remaining = buffered ++ input
    var expected = expectedPayloadBytes
    val decoded = Vector.newBuilder[Vector[Byte]]
    var failure: Option[FrameFailure] = None
    var continue = true

    while continue && failure.isEmpty do
      expected match
        case None if remaining.size >= FrameCodec.HeaderBytes =>
          val length = FrameDecoder.readLength(remaining)
          remaining = remaining.drop(FrameCodec.HeaderBytes)
          if length <= 0 then failure = Some(FrameFailure.InvalidLength(length))
          else if length > limits.maximumPayloadBytes.value then
            failure = Some(FrameFailure.FrameTooLarge(length, limits.maximumPayloadBytes.value))
          else expected = Some(length)
        case Some(length) if remaining.size >= length =>
          decoded += remaining.take(length)
          remaining = remaining.drop(length)
          expected = None
        case _ => continue = false

    failure.toLeft((FrameDecoder(limits, remaining, expected), decoded.result()))

  def finish: Either[FrameFailure, Unit] =
    if buffered.isEmpty && expectedPayloadBytes.isEmpty then Right(())
    else
      Left(
        FrameFailure.TruncatedFrame(
          bufferedBytes = buffered.size,
          expectedPayloadBytes = expectedPayloadBytes
        )
      )

object FrameDecoder:
  def empty(limits: FrameLimits = FrameLimits.default): FrameDecoder =
    FrameDecoder(limits, Vector.empty, None)

  private def readLength(bytes: Vector[Byte]): Int =
    ((bytes(0) & 0xff) << 24) |
      ((bytes(1) & 0xff) << 16) |
      ((bytes(2) & 0xff) << 8) |
      (bytes(3) & 0xff)
