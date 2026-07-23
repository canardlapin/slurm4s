package io.github.bbuchsbaum.scalaslurm.ssh

import io.github.bbuchsbaum.scalaslurm.core.DurationMillis
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure

object SshTarget:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("sshTarget", "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure("sshTarget", "must not be empty"))
    else if raw.startsWith("-") then Left(ValidationFailure("sshTarget", "must not begin with '-'"))
    else if raw.length > 512 then
      Left(ValidationFailure("sshTarget", "must contain at most 512 characters"))
    else if raw.exists(character => character.isControl || character.isWhitespace) then
      Left(ValidationFailure("sshTarget", "must not contain whitespace or control characters"))
    else Right(raw)

  extension (target: Type) def value: String = target

type SshTarget = SshTarget.Type

enum SshOption derives CanEqual:
  case ConfigFile(path: String)
  case Port(value: Int)
  case IdentityFile(path: String)
  case ProxyJump(target: SshTarget)
  case ConnectTimeout(value: DurationMillis)

final case class SshLaunch(
    executablePath: String,
    arguments: Vector[String]
) derives CanEqual

final case class SshConnection(
    executablePath: String,
    target: SshTarget,
    options: Vector[SshOption] = Vector.empty
) derives CanEqual

object SshCommand:
  private val fixedRemoteCommand = Vector("scala-slurm-agent", "serve", "--stdio")

  def agent(connection: SshConnection): Either[ValidationFailure, SshLaunch] =
    validateExecutable(connection.executablePath).flatMap { executable =>
      connection.options
        .foldLeft[Either[ValidationFailure, Vector[String]]](Right(Vector.empty)) {
          case (result, option) => result.flatMap(arguments => encode(option).map(arguments ++ _))
        }
        .map { options =>
          SshLaunch(
            executablePath = executable,
            arguments = Vector("-T", "-o", "RequestTTY=no") ++
              options ++
              Vector(connection.target.value) ++
              fixedRemoteCommand
          )
        }
    }

  private def encode(option: SshOption): Either[ValidationFailure, Vector[String]] = option match
    case SshOption.ConfigFile(path) =>
      validateOptionValue("sshConfigFile", path).map(value => Vector("-F", value))
    case SshOption.Port(value) =>
      Either.cond(
        value >= 1 && value <= 65535,
        Vector("-p", value.toString),
        ValidationFailure("sshPort", "must be between 1 and 65535")
      )
    case SshOption.IdentityFile(path) =>
      validateOptionValue("sshIdentityFile", path).map(value => Vector("-i", value))
    case SshOption.ProxyJump(target)     => Right(Vector("-J", target.value))
    case SshOption.ConnectTimeout(value) =>
      val seconds = math.max(1L, (value.value + 999L) / 1000L)
      Right(Vector("-o", s"ConnectTimeout=$seconds"))

  private def validateExecutable(raw: String): Either[ValidationFailure, String] =
    validateOptionValue("sshExecutablePath", raw)

  private def validateOptionValue(
      field: String,
      raw: String
  ): Either[ValidationFailure, String] =
    if raw == null then Left(ValidationFailure(field, "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure(field, "must not be empty"))
    else if raw.exists(_.isControl) then
      Left(ValidationFailure(field, "must not contain control characters"))
    else Right(raw)
