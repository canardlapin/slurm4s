import sbt.*

object Dependencies {
  object Versions {
    // Publication baseline. Scala recommends libraries publish from the LTS line: a compiler
    // consumes TASTy from its own or an EARLIER minor line, so publishing from 3.7 excluded every
    // 3.3 consumer.
    val scala3 = "3.3.8"
    // Verification-only lane for the current regular release.
    val scala3Next = "3.8.4"
    val cats = "2.13.0"
    val catsEffect = "3.7.0"
    val fs2 = "3.13.0"
    val circe = "0.14.16"
    val scodecBits = "1.2.4"
    val munit = "1.3.0"
    val munitCatsEffect = "2.2.0"
    val scalaCheck = "1.19.0"
    val disciplineMunit = "2.0.0"
  }

  object Libraries {
    val catsCore = "org.typelevel" %% "cats-core" % Versions.cats
    val catsEffect = "org.typelevel" %% "cats-effect" % Versions.catsEffect
    val fs2Core = "co.fs2" %% "fs2-core" % Versions.fs2
    val fs2Io = "co.fs2" %% "fs2-io" % Versions.fs2
    val circeCore = "io.circe" %% "circe-core" % Versions.circe
    val circeGeneric = "io.circe" %% "circe-generic" % Versions.circe
    val circeParser = "io.circe" %% "circe-parser" % Versions.circe
    val scodecBits = "org.scodec" %% "scodec-bits" % Versions.scodecBits
    val munit = "org.scalameta" %% "munit" % Versions.munit
    val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % Versions.munit
    val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % Versions.munitCatsEffect
    val scalaCheck = "org.scalacheck" %% "scalacheck" % Versions.scalaCheck
    val catsLaws = "org.typelevel" %% "cats-laws" % Versions.cats
    val disciplineMunit = "org.typelevel" %% "discipline-munit" % Versions.disciplineMunit
  }
}
