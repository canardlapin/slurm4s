package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.kernel.Sync
import io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles as KernelAtomicFiles
import io.github.bbuchsbaum.slurm4s.core.ContentDigest

import java.nio.file.Path

/** Source-compatible worker facade for the scheduler-neutral atomic filesystem kernel.
  *
  * New scheduler-neutral consumers should import
  * `io.github.bbuchsbaum.remoteexec.kernel.AtomicFiles` directly. This facade remains while
  * downstream users migrate.
  */
object AtomicFiles:
  type WriteFailure = KernelAtomicFiles.WriteFailure
  val WriteFailure: KernelAtomicFiles.WriteFailure.type = KernelAtomicFiles.WriteFailure

  type ClaimFailure = KernelAtomicFiles.ClaimFailure
  val ClaimFailure: KernelAtomicFiles.ClaimFailure.type = KernelAtomicFiles.ClaimFailure

  def writeNewBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    KernelAtomicFiles.writeNewBlocking(target, bytes, executable)

  def writeNew[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    KernelAtomicFiles.writeNew(target, bytes, executable)

  def writeStableBlocking(
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): Either[WriteFailure, Unit] =
    KernelAtomicFiles.writeStableBlocking(target, bytes, executable)

  def writeStable[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte],
      executable: Boolean = false
  ): F[Either[WriteFailure, Unit]] =
    KernelAtomicFiles.writeStable(target, bytes, executable)

  def replaceBlocking(target: Path, bytes: Vector[Byte]): Either[WriteFailure, Unit] =
    KernelAtomicFiles.replaceBlocking(target, bytes)

  def replace[F[_]: Sync](target: Path, bytes: Vector[Byte]): F[Either[WriteFailure, Unit]] =
    KernelAtomicFiles.replace(target, bytes)

  def publishOnceBlocking(
      target: Path,
      bytes: Vector[Byte]
  ): Either[WriteFailure, ContentDigest] =
    KernelAtomicFiles.publishOnceBlocking(target, bytes)

  def publishOnce[F[_]: Sync](
      target: Path,
      bytes: Vector[Byte]
  ): F[Either[WriteFailure, ContentDigest]] =
    Sync[F].blocking(publishOnceBlocking(target, bytes))

  def claimBlocking(from: Path, to: Path): Either[ClaimFailure, Path] =
    KernelAtomicFiles.claimBlocking(from, to)

  def claim[F[_]: Sync](from: Path, to: Path): F[Either[ClaimFailure, Path]] =
    KernelAtomicFiles.claim(from, to)

  def digestOf(bytes: Vector[Byte]): ContentDigest =
    KernelAtomicFiles.digestOf(bytes)
