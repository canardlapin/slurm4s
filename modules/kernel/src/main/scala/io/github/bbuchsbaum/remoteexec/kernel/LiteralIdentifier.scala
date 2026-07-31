package io.github.bbuchsbaum.remoteexec.kernel

import scala.quoted.*

/** Compile-time validation for identifier literals.
  *
  * A literal is known at compile time, so a runtime `Either` for it is ceremony that callers pay
  * for and then discard — which is why the flagship example opened with four consecutive
  * `.from(...).toOption.get` calls, teaching partial-function unwrapping as the idiom.
  *
  * Dynamic values still return `Either`, because they genuinely can fail. Only literals are checked
  * here, and an invalid one is a compile error rather than a runtime throw.
  */
object LiteralIdentifier:

  /** Validate a string literal against the same rules `TextIdentifier` applies at runtime. */
  inline def text(inline field: String, inline maximumLength: Int, inline raw: String): String =
    ${ textImpl('field, 'maximumLength, 'raw) }

  private def textImpl(
      field: Expr[String],
      maximumLength: Expr[Int],
      raw: Expr[String]
  )(using Quotes): Expr[String] =
    import quotes.reflect.*
    (field.value, maximumLength.value, raw.value) match
      case (Some(fieldValue), Some(limit), Some(rawValue)) =>
        IdentifierRules.text(fieldValue, rawValue, limit) match
          case Right(_)      => Expr(rawValue)
          case Left(problem) =>
            report.errorAndAbort(s"invalid ${problem.field} literal: ${problem.reason}")
      case (_, _, None) =>
        report.errorAndAbort(
          "this constructor accepts only a string literal; use `from` for a dynamic value"
        )
      case _ =>
        report.errorAndAbort("identifier field and length must be literals")
