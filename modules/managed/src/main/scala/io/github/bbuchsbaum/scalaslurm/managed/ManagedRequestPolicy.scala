package io.github.bbuchsbaum.scalaslurm.managed

import io.github.bbuchsbaum.scalaslurm.core.*

trait ManagedRequestPolicy:
  def validate[A](request: JobRequest[A]): Either[Diagnostics, Unit]

object ManagedRequestPolicy:
  val rejectEnvironmentValues: ManagedRequestPolicy = new ManagedRequestPolicy:
    def validate[A](request: JobRequest[A]): Either[Diagnostics, Unit] =
      Either.cond(
        request.environment.isEmpty,
        (),
        Diagnostics.one(
          Diagnostic(
            "durable-environment-values-forbidden",
            "managed requests do not persist environment values unless a public-name policy is explicit",
            Map("names" -> displayNames(request.environment.keySet))
          )
        )
      )

  def publicEnvironment(
      allowedNames: Set[String]
  ): Either[ValidationFailure, ManagedRequestPolicy] =
    val invalid = allowedNames.toVector.filterNot(validName).map(displayName).sorted
    if invalid.nonEmpty then
      Left(
        ValidationFailure(
          "durableEnvironmentPolicy",
          s"contains invalid environment names: ${invalid.mkString(", ")}"
        )
      )
    else if allowedNames.size > MaximumNames then
      Left(
        ValidationFailure(
          "durableEnvironmentPolicy",
          s"must contain at most $MaximumNames public names"
        )
      )
    else Right(PublicEnvironment(allowedNames))

  final private case class PublicEnvironment(allowedNames: Set[String])
      extends ManagedRequestPolicy:
    def validate[A](request: JobRequest[A]): Either[Diagnostics, Unit] =
      val names = request.environment.keySet
      val invalidNames = names.toVector.filterNot(validName)
      val undeclaredNames = names.diff(allowedNames)
      val nullValues = request.environment.iterator.collect { case (name, null) =>
        name
      }.toVector
      val oversizedValues = request.environment.iterator.collect {
        case (name, value) if value != null && value.length > MaximumValueCharacters => name
      }.toVector
      val nulValues = request.environment.iterator.collect {
        case (name, value) if value != null && value.indexOf('\u0000') >= 0 => name
      }.toVector
      val problems = Vector(
        diagnostic(
          invalidNames,
          "durable-environment-name-invalid",
          "managed environment names must use portable process-variable syntax"
        ),
        diagnostic(
          undeclaredNames,
          "durable-environment-name-not-public",
          "managed environment values require an explicitly declared non-secret public name"
        ),
        diagnostic(
          nullValues,
          "durable-environment-value-null",
          "managed environment values must not be null"
        ),
        diagnostic(
          oversizedValues,
          "durable-environment-value-too-large",
          s"managed environment values must contain at most $MaximumValueCharacters characters"
        ),
        diagnostic(
          nulValues,
          "durable-environment-value-invalid",
          "managed environment values must not contain NUL"
        )
      ).flatten
      Diagnostics.fromVector(problems) match
        case Right(diagnostics) => Left(diagnostics)
        case Left(_)            => Right(())

  private val MaximumNames = 256
  private val MaximumValueCharacters = 64 * 1024
  private val NamePattern = "[A-Za-z_][A-Za-z0-9_]{0,254}".r

  private def validName(value: String): Boolean =
    value != null && NamePattern.matches(value)

  private def diagnostic(
      names: Iterable[String],
      code: String,
      message: String
  ): Option[Diagnostic] =
    Option.when(names.nonEmpty)(
      Diagnostic(code, message, Map("names" -> displayNames(names)))
    )

  private def displayNames(names: Iterable[String]): String =
    names.iterator.map(displayName).toVector.distinct.sorted.mkString(",")

  private def displayName(value: String): String =
    Option(value).getOrElse("<null>")
