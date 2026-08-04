package io.github.bbuchsbaum.remoteexec.kernel

import scodec.bits.ByteVector

import java.nio.file.Path
import java.nio.file.Paths

/** A separate-process contender in an atomic publication race.
  *
  * Same-JVM fibers cannot exercise this: `AtomicFiles` serializes callers within one process via a
  * striped monitor, so an in-process test measures that monitor rather than the cross-process
  * sidecar lock. Only a real second JVM contends for the lock the way a worker on another node
  * would.
  *
  * Prints exactly one line — `WON`, or `LOST:<failure>` — so the parent can count outcomes.
  */
object AtomicRaceWorker:
  def main(args: Array[String]): Unit =
    val operation = args(0)
    val target: Path = Paths.get(args(1))
    val payload = ByteVector.view(args(2).getBytes("UTF-8"))

    val line = operation match
      case "publishOnce" =>
        AtomicFiles.publishOnceBlocking(target, payload) match
          case Right(_)      => "WON"
          case Left(failure) => s"LOST:$failure"
      case "writeNew" =>
        AtomicFiles.writeNewBlocking(target, payload) match
          case Right(_)      => "WON"
          case Left(failure) => s"LOST:$failure"
      case "writeStable" =>
        AtomicFiles.writeStableBlocking(target, payload) match
          case Right(_)      => "WON"
          case Left(failure) => s"LOST:$failure"
      case "claim" =>
        // args(3) is this contender's private destination; the source is the contested file.
        val destination = Paths.get(args(3))
        AtomicFiles.claimBlocking(target, destination) match
          case Right(_)      => "WON"
          case Left(failure) => s"LOST:$failure"
      case other => s"LOST:unknown operation $other"

    Console.out.println(line)
    Console.out.flush()
