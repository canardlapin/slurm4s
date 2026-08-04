package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.PositiveInt

import java.nio.file.Files
import java.nio.file.Path
import scala.concurrent.duration.*

/** A one-way notification that a worker should begin its application-defined drain procedure.
  *
  * This is deliberately only an adapter seam. It does not install a JVM signal handler, cancel a
  * task, or decide what draining means. A supervising launcher may translate a scheduler warning
  * into this source; task or pilot code remains responsible for stopping admission and publishing
  * any final result.
  *
  * Callers must tolerate early delivery. Slurm may send an advance signal before the requested lead
  * time, and this notice is distinct from the later terminal SIGTERM/SIGKILL sequence.
  */
trait DrainNoticeSource[F[_]]:
  def await: F[Unit]

object DrainNoticeSource:
  /** Adapts an injected notification effect, such as a `Deferred.get`. */
  def injected[F[_]](awaitNotice: F[Unit]): DrainNoticeSource[F] =
    new DrainNoticeSource[F]:
      def await: F[Unit] = awaitNotice

  /** Watches for a marker created by an external signal-aware supervisor.
    *
    * The marker is neither deleted nor interpreted. Keeping signal handling out of this adapter
    * avoids implying that receiving a Slurm warning is equivalent to graceful JVM shutdown.
    */
  def file(marker: Path, pollEveryMillis: PositiveInt): DrainNoticeSource[IO] =
    val normalized = marker.toAbsolutePath.normalize()
    new DrainNoticeSource[IO]:
      def await: IO[Unit] =
        def loop: IO[Unit] =
          IO.blocking(Files.exists(normalized)).flatMap { exists =>
            if exists then IO.unit
            else IO.sleep(pollEveryMillis.toInt.millis) *> loop
          }
        loop
