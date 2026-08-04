package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.IO
import cats.effect.std.Semaphore
import io.github.bbuchsbaum.slurm4s.core.ByteLimit
import io.github.bbuchsbaum.slurm4s.core.WorkerEvent
import io.github.bbuchsbaum.slurm4s.protocol.WorkerEventCodec

import scodec.bits.ByteVector

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions

object FileWorkerEventSink:
  def create(target: Path, maximumEventBytes: ByteLimit): IO[WorkerEventSink[IO]] =
    Semaphore[IO](1L).map { semaphore =>
      new WorkerEventSink[IO]:
        def append(event: WorkerEvent): IO[Unit] =
          WorkerEventCodec.encode(event, maximumEventBytes) match
            case Left(failure) =>
              IO.raiseError(new IllegalArgumentException(s"worker event cannot encode: $failure"))
            case Right(bytes) =>
              semaphore.permit.use(_ => IO.blocking(appendLocked(target, bytes)))
    }

  private def appendLocked(target: Path, bytes: ByteVector): Unit =
    val normalized = target.toAbsolutePath.normalize()
    Option(normalized.getParent).foreach { parent =>
      val _ = Files.createDirectories(parent)
    }
    val channel = FileChannel.open(
      normalized,
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.APPEND
    )
    setPrivate(normalized)
    try
      val lock = channel.lock()
      try
        val buffer = ByteBuffer.wrap(bytes.toArray)
        while buffer.hasRemaining do
          val _ = channel.write(buffer)
        channel.force(true)
      finally lock.release()
    finally channel.close()

  private def setPrivate(path: Path): Unit =
    try
      val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
    catch case _: UnsupportedOperationException => ()
