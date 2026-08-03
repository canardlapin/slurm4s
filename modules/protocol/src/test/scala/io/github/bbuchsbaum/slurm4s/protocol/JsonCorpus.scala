package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json

import org.scalacheck.Gen

/** The hostile-input corpora the malformed-input laws draw from (P6f.34).
  *
  * Shared rather than suite-local because the same three corpora apply to every hand-written
  * decoder in the repository, wire and journal alike, and a decoder that is only tested against one
  * of them has a blind spot in the other two.
  */
object JsonCorpus:

  /** Arbitrary JSON, bounded in depth so generation terminates.
    *
    * Checks the outer shape. On its own it is weak: it usually fails at the first field and so
    * never reaches the interesting code, which is why [[mutations]] exists.
    */
  val arbitraryJson: Gen[Json] =
    def loop(depth: Int): Gen[Json] =
      val leaf = Gen.oneOf(
        Gen.const(Json.Null),
        Gen.oneOf(true, false).map(Json.fromBoolean),
        Gen.choose(-1_000_000L, 1_000_000L).map(Json.fromLong),
        Gen
          .oneOf("", "0", "-1", "kind", "unknown-discriminator", "\u00ff\u0000")
          .map(Json.fromString)
      )
      if depth <= 0 then leaf
      else
        Gen.oneOf(
          leaf,
          Gen.choose(0, 3).flatMap(Gen.listOfN(_, loop(depth - 1))).map(Json.fromValues),
          Gen
            .choose(0, 3)
            .flatMap(
              Gen.listOfN(
                _,
                Gen.zip(
                  Gen.oneOf("kind", "type", "job", "state", "evidence", "value", "bytes"),
                  loop(depth - 1)
                )
              )
            )
            .map(fields => Json.obj(fields*))
        )
    loop(3)

  /** Structure-preserving corruptions of a valid encoding.
    *
    * These reach decoders that have already gotten past their discriminator, which is where partial
    * reads and unchecked casts actually live.
    */
  def mutations(json: Json): Gen[Json] =
    json.asObject match
      case None      => Gen.const(json)
      case Some(obj) =>
        val keys = obj.keys.toVector
        if keys.isEmpty then Gen.const(json)
        else
          Gen.oneOf(keys).flatMap { key =>
            Gen.oneOf(
              Gen.const(Json.fromJsonObject(obj.remove(key))),
              Gen.const(Json.fromJsonObject(obj.add(key, Json.Null))),
              Gen.const(Json.fromJsonObject(obj.add(key, Json.fromString("not-a-structure")))),
              Gen.const(Json.fromJsonObject(obj.add(key, Json.arr()))),
              Gen.const(Json.fromJsonObject(obj.add(key, Json.fromLong(-1L))))
            )
          }

  /** Corrupts a field one level down instead of at the top, so the discriminator still matches. */
  def nestedMutations(json: Json): Gen[Json] =
    json.asObject.flatMap(obj => obj.keys.headOption.map(key => (obj, key))) match
      case None             => Gen.const(json)
      case Some((obj, key)) =>
        mutations(obj(key).getOrElse(Json.Null))
          .map(mutated => Json.fromJsonObject(obj.add(key, mutated)))

  /** A field name no codec in this repository reads, as a newer peer would add. */
  val UnknownField = "fieldFromANewerPeer"

  /** Inserts an unknown field at every depth of an object tree, leaving existing fields intact. */
  def withUnknownField(json: Json): Gen[Json] =
    json.asObject match
      case None      => Gen.const(json)
      case Some(obj) =>
        val here = Gen.const(Json.fromJsonObject(obj.add(UnknownField, Json.fromString("ignored"))))
        val keys = obj.keys.toVector.filter(key => obj(key).exists(_.isObject))
        if keys.isEmpty then here
        else
          Gen.oneOf(
            here,
            Gen.oneOf(keys).flatMap { key =>
              withUnknownField(obj(key).getOrElse(Json.Null))
                .map(nested => Json.fromJsonObject(obj.add(key, nested)))
            }
          )
