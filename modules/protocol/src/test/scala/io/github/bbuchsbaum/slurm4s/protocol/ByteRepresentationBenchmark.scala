package io.github.bbuchsbaum.slurm4s.protocol

/** Measures what `Vector[Byte]` actually costs, as opposed to what it is assumed to cost.
  *
  * The frame-accumulation benchmark showed that `FrameDecoder.feed` does not copy the payload,
  * because a Scala `Vector` concatenates and slices through a shared trie. That leaves two
  * candidate costs worth measuring before changing the representation across the codebase: the
  * retained footprint, and the unboxing paid every time bytes cross into an array, a buffer, or a
  * digest.
  *
  * sbt "protocol/Test/runMain io.github.bbuchsbaum.slurm4s.protocol.ByteRepresentationBenchmark"
  */
object ByteRepresentationBenchmark:
  private val PayloadBytes = 4 * 1024 * 1024
  private val WarmupIterations = 3
  private val MeasuredIterations = 10

  def main(arguments: Array[String]): Unit =
    footprint()
    conversion()

  private def footprint(): Unit =
    println("-- retained footprint of one 4 MiB payload --")
    val asArray = measureRetained("Array[Byte] ")(() => Array.tabulate(PayloadBytes)(byteAt))
    val asVector = measureRetained("Vector[Byte]")(() => Vector.tabulate(PayloadBytes)(byteAt))
    if asArray > 0L then
      println(f"Vector/Array ratio: ${asVector.toDouble / asArray.toDouble}%.1fx")

  /** Allocates through `build`, holding the result live across the measurement. */
  private def measureRetained(label: String)(build: () => AnyRef): Long =
    val runtime = Runtime.getRuntime
    settle()
    val before = runtime.totalMemory() - runtime.freeMemory()
    val retained = build()
    settle()
    val after = runtime.totalMemory() - runtime.freeMemory()
    // Touching the value after the second reading keeps it reachable, so the delta is its retained
    // size rather than whatever the collector decided it could drop.
    val bytes = if retained.hashCode() == Int.MinValue then -1L else after - before
    println(f"$label: ${bytes / (1024.0 * 1024.0)}%6.1f MiB")
    bytes

  private def conversion(): Unit =
    println("-- cost of handing 4 MiB to an array, which every codec and digest boundary pays --")
    val vector = Vector.tabulate(PayloadBytes)(byteAt)
    val array = Array.tabulate(PayloadBytes)(byteAt)
    time("Vector[Byte].toArray")(() => vector.toArray.length.toLong)
    time("Array[Byte].clone   ")(() => array.clone().length.toLong)

  private def time(label: String)(operation: () => Long): Unit =
    var sink = 0L
    (0 until WarmupIterations).foreach(_ => sink += operation())
    val samples = (0 until MeasuredIterations).map { _ =>
      val startedAt = System.nanoTime()
      sink += operation()
      System.nanoTime() - startedAt
    }.sorted
    val fastest = samples.head
    val throughput = (PayloadBytes.toDouble / (1024 * 1024)) / (fastest.toDouble / 1e9)
    println(f"$label: ${fastest / 1e6}%7.2f ms  $throughput%8.0f MiB/s")
    if sink == 0L then println("  MISMATCH: the harness measured nothing")

  private def byteAt(index: Int): Byte = (index % 251).toByte

  /** Best effort at a stable heap reading. Not exact, which is why only a ratio is reported. */
  private def settle(): Unit =
    (0 until 3).foreach { _ =>
      System.gc()
      Thread.sleep(60L)
    }
