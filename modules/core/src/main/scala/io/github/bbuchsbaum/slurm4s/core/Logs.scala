package io.github.bbuchsbaum.slurm4s.core

import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier

import java.time.Instant

enum LogStream derives CanEqual:
  case Stdout
  case Stderr

object FileIdentity extends TextIdentifier("fileIdentity", 512)
type FileIdentity = FileIdentity.Type

final case class LogRef(
    attemptId: AttemptId,
    epoch: AttemptEpoch,
    stream: LogStream,
    locator: String
) derives CanEqual

final case class LogCursor(offset: LogOffset, fileIdentity: Option[FileIdentity]) derives CanEqual

object LogCursor:
  val start: LogCursor = LogCursor(LogOffset.start, None)

final case class LogPage(
    bytes: Vector[Byte],
    next: LogCursor,
    endOfFile: Boolean,
    observedAt: Instant
) derives CanEqual

enum LogReadResult derives CanEqual:
  case Page(value: LogPage)
  case WaitingForFile(cursor: LogCursor, observedAt: Instant)
  case CursorInvalid(
      requested: LogCursor,
      currentIdentity: Option[FileIdentity],
      currentSize: Long,
      observedAt: Instant
  )
  case Failed(diagnostics: Diagnostics, observedAt: Instant)
