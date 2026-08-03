package io.github.bbuchsbaum.slurm4s.protocol

/** Measures `FrameDecoder.feed` over one frame delivered in 64 KiB chunks.
  *
  * This is the scenario named in the acceptance criteria for the byte-representation change, and it
  * exists so the claimed win is measured rather than asserted. It is not a munit suite: a timing
  * harness inside the suite would make the suite's runtime depend on machine load. Run it with
  *
  * sbt "protocol/Test/runMain io.github.bbuchsbaum.slurm4s.protocol.FrameDecoderBenchmark"
  *
  * It sweeps payload sizes rather than timing one, because the per-chunk accumulation cost is the
  * thing in question and only its scaling can tell a quadratic concatenation from a linear one.
  * Doubling the chunk count should roughly double a linear cost and roughly quadruple a quadratic
  * one. Absolute numbers are comparable only within one machine and JDK.
  */
object FrameDecoderBenchmark:
  private val ChunkBytes = 64 * 1024
  private val WarmupIterations = 5
  private val MeasuredIterations = 10

  def main(arguments: Array[String]): Unit =
    println("payload      chunks    fastest     median      MiB/s")
    Vector(512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024).foreach(report)

  private def report(payloadBytes: Int): Unit =
    val chunks = frameChunks(payloadBytes)

    // Deliberately consumed rather than discarded: a wrong total means the harness stopped
    // measuring what it claims to, and consuming it keeps the decode from being optimised away.
    var observedPayloadBytes = 0L
    (0 until WarmupIterations).foreach(_ => observedPayloadBytes += decodeOnce(chunks))

    val samples = (0 until MeasuredIterations).map { _ =>
      val startedAt = System.nanoTime()
      observedPayloadBytes += decodeOnce(chunks)
      System.nanoTime() - startedAt
    }.sorted

    val fastest = samples.head
    val median = samples(samples.size / 2)
    val expected = (WarmupIterations + MeasuredIterations).toLong * payloadBytes.toLong
    val throughput = (payloadBytes.toDouble / (1024 * 1024)) / (fastest.toDouble / 1e9)

    println(
      f"${payloadBytes / 1024}%5d KiB  ${chunks.size}%7d  ${fastest / 1e6}%7.2f ms  " +
        f"${median / 1e6}%7.2f ms  $throughput%9.0f"
    )
    if observedPayloadBytes != expected then
      println(s"  MISMATCH: decoded $observedPayloadBytes, expected $expected")

  /** One encoded frame of `payloadBytes`, split into the chunks a transport would hand over. */
  private def frameChunks(payloadBytes: Int): Vector[Vector[Byte]] =
    val payload = Vector.tabulate(payloadBytes)(index => (index % 251).toByte)
    val frame = FrameCodec
      .encode(payload, FrameLimits.default)
      .fold(
        failure => throw new IllegalStateException(s"benchmark frame rejected: $failure"),
        identity
      )
    frame.grouped(ChunkBytes).toVector

  /** Feeds every chunk through one decoder, returning the total decoded payload size. */
  private def decodeOnce(chunks: Vector[Vector[Byte]]): Long =
    val (_, total) = chunks.foldLeft(FrameDecoder.empty() -> 0L) {
      case ((decoder, accumulated), chunk) =>
        decoder.feed(chunk) match
          case Left(failure) =>
            throw new IllegalStateException(s"benchmark decode failed: $failure")
          case Right((next, frames)) =>
            next -> (accumulated + frames.map(_.size.toLong).sum)
    }
    total
