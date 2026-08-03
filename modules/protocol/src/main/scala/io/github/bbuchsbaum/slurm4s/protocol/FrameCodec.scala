package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import scodec.bits.ByteOrdering
import scodec.bits.ByteVector

final case class FrameLimits(maximumPayloadBytes: ByteLimit) derives CanEqual

object FrameLimits:
  val default: FrameLimits = FrameLimits(ByteLimit.maximumCommandCapture)

enum FrameFailure derives CanEqual:
  case EmptyPayload

  /** A length read from a frame header, so genuinely 32-bit. */
  case InvalidLength(received: Int)
  case FrameTooLarge(received: Long, maximum: Int)
  case TruncatedFrame(bufferedBytes: Long, expectedPayloadBytes: Option[Int])

object FrameCodec:
  val HeaderBytes: Int = 4

  def encode(payload: ByteVector, limits: FrameLimits): Either[FrameFailure, ByteVector] =
    val length = payload.size
    if length == 0L then Left(FrameFailure.EmptyPayload)
    else if length > limits.maximumPayloadBytes.value.toLong then
      Left(FrameFailure.FrameTooLarge(length, limits.maximumPayloadBytes.value))
    else
      // Narrowing is safe under the bound just checked. The ordering is stated rather than left to
      // a default because it is the wire contract, not an implementation preference.
      Right(
        ByteVector.fromInt(
          length.toInt,
          size = HeaderBytes,
          ordering = ByteOrdering.BigEndian
        ) ++ payload
      )

final case class FrameDecoder private (
    limits: FrameLimits,
    buffered: ByteVector,
    expectedPayloadBytes: Option[Int]
):
  def feed(input: ByteVector): Either[FrameFailure, (FrameDecoder, Vector[ByteVector])] =
    var remaining = buffered ++ input
    var expected = expectedPayloadBytes
    val decoded = Vector.newBuilder[ByteVector]
    var failure: Option[FrameFailure] = None
    var continue = true

    while continue && failure.isEmpty do
      expected match
        case None if remaining.size >= FrameCodec.HeaderBytes.toLong =>
          val length = FrameDecoder.readLength(remaining)
          remaining = remaining.drop(FrameCodec.HeaderBytes.toLong)
          if length <= 0 then failure = Some(FrameFailure.InvalidLength(length))
          else if length > limits.maximumPayloadBytes.value then
            failure = Some(
              FrameFailure.FrameTooLarge(length.toLong, limits.maximumPayloadBytes.value)
            )
          else expected = Some(length)
        case Some(length) if remaining.size >= length.toLong =>
          decoded += remaining.take(length.toLong)
          remaining = remaining.drop(length.toLong)
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
    FrameDecoder(limits, ByteVector.empty, None)

  /** Reads the big-endian header length. Signed, so a header claiming 2 GiB or more arrives as a
    * negative value and is rejected as an invalid length rather than silently believed.
    */
  private def readLength(bytes: ByteVector): Int =
    bytes
      .take(FrameCodec.HeaderBytes.toLong)
      .toInt(signed = true, ordering = ByteOrdering.BigEndian)
