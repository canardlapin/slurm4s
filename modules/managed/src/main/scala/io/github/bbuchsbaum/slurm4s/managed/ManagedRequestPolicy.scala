package io.github.bbuchsbaum.slurm4s.managed

import io.github.bbuchsbaum.slurm4s.core.*

trait ManagedRequestPolicy:
  def validate(spec: LaunchSpec): Either[Diagnostics, Unit]

object ManagedRequestPolicy:
  val rejectEnvironmentValues: ManagedRequestPolicy = new ManagedRequestPolicy:
    def validate(spec: LaunchSpec): Either[Diagnostics, Unit] =
      Either.cond(
        spec.environment.isEmpty,
        (),
        Diagnostics.one(
          Diagnostic(
            "durable-environment-values-forbidden",
            "managed requests do not persist environment values unless a public-name policy is explicit",
            Map("names" -> displayNames(spec.environment.keySet))
          )
        )
      )

  def publicEnvironment(
      allowedNames: Set[String]
  ): Either[ValidationFailure, ManagedRequestPolicy] =
    val validated = allowedNames.toVector.map(raw => raw -> EnvName.from(raw))
    val invalid = validated.collect { case (raw, Left(_)) => displayRawName(raw) }.sorted
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
    else Right(PublicEnvironment(validated.flatMap(_._2.toOption).toSet))

  final private case class PublicEnvironment(allowedNames: Set[EnvName])
      extends ManagedRequestPolicy:
    def validate(spec: LaunchSpec): Either[Diagnostics, Unit] =
      val names = spec.environment.keySet
      val undeclaredNames = names.diff(allowedNames)
      val nullValues = spec.environment.iterator.collect { case (name, null) =>
        name
      }.toVector
      val oversizedValues = spec.environment.iterator.collect {
        case (name, value) if value != null && value.length > MaximumValueCharacters => name
      }.toVector
      val nulValues = spec.environment.iterator.collect {
        case (name, value) if value != null && value.indexOf('\u0000') >= 0 => name
      }.toVector
      val problems = Vector(
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

  private def diagnostic(
      names: Iterable[EnvName],
      code: String,
      message: String
  ): Option[Diagnostic] =
    Option.when(names.nonEmpty)(
      Diagnostic(code, message, Map("names" -> displayNames(names)))
    )

  private def displayNames(names: Iterable[EnvName]): String =
    names.iterator.map(_.value).toVector.distinct.sorted.mkString(",")

  private def displayRawName(value: String): String =
    Option(value).getOrElse("<null>")
