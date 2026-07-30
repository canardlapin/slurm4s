package io.github.bbuchsbaum.slurm4s.ssh

import cats.Order
import cats.Show
import io.github.bbuchsbaum.slurm4s.core.DurationMillis
import io.github.bbuchsbaum.slurm4s.core.ValidationFailure

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
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

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

/** Whether OpenSSH may consult an interactive authentication method.
  *
  * Library calls default to [[NonInteractive]] so a missing or refused credential becomes an
  * ordinary process result instead of waiting on a password or keyboard-interactive prompt.
  * [[ConfiguredInteractive]] is an explicit compatibility mode: the calling application owns the
  * controlling terminal and must keep the exchange timeout bounded.
  */
enum SshAuthentication derives CanEqual:
  case NonInteractive
  case ConfiguredInteractive

final case class SshConnection(
    executablePath: String,
    target: SshTarget,
    options: Vector[SshOption] = Vector.empty,
    authentication: SshAuthentication = SshAuthentication.NonInteractive
) derives CanEqual

object SshCommand:
  private val fixedRemoteCommand = Vector("slurm4s-agent", "serve", "--stdio")

  def agent(connection: SshConnection): Either[ValidationFailure, SshLaunch] =
    validateExecutable(connection.executablePath).flatMap { executable =>
      connection.options
        .foldLeft[Either[ValidationFailure, Vector[String]]](Right(Vector.empty)) {
          case (result, option) => result.flatMap(arguments => encode(option).map(arguments ++ _))
        }
        .map { options =>
          val authentication = connection.authentication match
            case SshAuthentication.NonInteractive =>
              Vector("-o", "BatchMode=yes")
            case SshAuthentication.ConfiguredInteractive =>
              Vector("-o", "BatchMode=no")
          SshLaunch(
            executablePath = executable,
            arguments = Vector("-T", "-o", "RequestTTY=no") ++
              authentication ++
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
