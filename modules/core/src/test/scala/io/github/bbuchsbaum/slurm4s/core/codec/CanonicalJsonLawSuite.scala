package io.github.bbuchsbaum.slurm4s.core.codec

import io.circe.Json
import io.circe.JsonObject
import io.circe.parser

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** Laws for the canonical JSON rendering (P7.4).
  *
  * Canonicalization is a compatibility contract, not a formatting preference. The control journal
  * checksums a command's rendering, and a managed request's digest of its rendering *is* its
  * identity, so both depend on two independent renderings of equal JSON producing equal bytes.
  *
  * These laws pin the properties those consumers rely on. They are what a second printer,
  * reintroduced with different settings, would have to keep satisfying — and the point of stating
  * them is that a divergence in any one of them is otherwise silent: a checksum computed under one
  * printer and verified under another rejects a valid record without saying why.
  */
class CanonicalJsonLawSuite extends munit.ScalaCheckSuite:

  private val leaf: Gen[Json] =
    Gen.oneOf(
      Gen.const(Json.Null),
      Gen.oneOf(true, false).map(Json.fromBoolean),
      Gen.choose(-1_000_000L, 1_000_000L).map(Json.fromLong),
      Gen.oneOf("", "alpha", "0", "ünïcode", "with \"quotes\"").map(Json.fromString)
    )

  private val fieldName: Gen[String] =
    Gen.oneOf("zeta", "alpha", "Mixed", "b", "nested", "0numeric", "with space")

  /** Field lists rather than objects, so a test can render the same entries in a different order.
    */
  private def entries(depth: Int): Gen[Vector[(String, Json)]] =
    Gen
      .choose(0, 4)
      .flatMap(Gen.listOfN(_, Gen.zip(fieldName, json(depth))))
      .map(_.toVector.distinctBy(_._1))

  private def json(depth: Int): Gen[Json] =
    if depth <= 0 then leaf
    else
      Gen.oneOf(
        leaf,
        Gen.choose(0, 3).flatMap(Gen.listOfN(_, json(depth - 1))).map(Json.fromValues),
        entries(depth - 1).map(fields => Json.fromJsonObject(JsonObject.fromIterable(fields)))
      )

  private val anyJson: Gen[Json] = json(3)

  /** The load-bearing law. Two peers holding equal JSON must produce equal bytes, and JSON equality
    * does not constrain field order, so a rendering that preserved insertion order would make a
    * checksum depend on how the object happened to be built rather than on what it contains.
    */
  property("rendering does not depend on the order fields were added") {
    val scenario = entries(2).map { fields =>
      (
        Json.fromJsonObject(JsonObject.fromIterable(fields)),
        Json.fromJsonObject(JsonObject.fromIterable(fields.reverse))
      )
    }
    forAll(scenario) { (built, rebuilt) =>
      assertEquals(CanonicalJson.print(rebuilt), CanonicalJson.print(built))
    }
  }

  /** Nested objects too: sorting only the top level would still let two renderings of equal JSON
    * disagree, and the payloads these consumers hash are several levels deep.
    */
  property("rendering sorts field names at every depth") {
    forAll(anyJson) { value =>
      val text = CanonicalJson.print(value)
      assertEquals(
        text,
        CanonicalJson.print(parser.parse(text).toOption.get),
        "re-rendering a canonical text changed it, so some depth is not canonical"
      )
    }
  }

  /** An explicit null must stay distinguishable from an absent field. Dropping nulls would make
    * `{"a":null}` and `{}` share a digest, and the wire codecs use presence to mean something.
    */
  property("an explicit null renders and survives, rather than being dropped") {
    forAll(fieldName) { name =>
      val withNull = Json.obj(name -> Json.Null)
      assertNotEquals(CanonicalJson.print(withNull), CanonicalJson.print(Json.obj()))
      assertEquals(parser.parse(CanonicalJson.print(withNull)).toOption, Some(withNull))
    }
  }

  property("rendering carries no insignificant whitespace") {
    forAll(anyJson) { value =>
      val stripped = CanonicalJson
        .print(value)
        .replaceAll("\"(\\\\.|[^\"\\\\])*\"", "\"\"")
      assert(
        !stripped.exists(_.isWhitespace),
        s"whitespace survived outside a string literal: $stripped"
      )
    }
  }

  property("rendering is deterministic and preserves the value") {
    forAll(anyJson) { value =>
      assertEquals(CanonicalJson.print(value), CanonicalJson.print(value))
      assertEquals(parser.parse(CanonicalJson.print(value)).toOption, Some(value))
    }
  }

  property("the byte rendering is the UTF-8 encoding of the text rendering") {
    forAll(anyJson) { value =>
      assertEquals(CanonicalJson.bytes(value).decodeUtf8, Right(CanonicalJson.print(value)))
    }
  }

  /** Guards the laws above against being tested only on flat, ASCII, null-free objects, which is
    * where two printers would agree anyway.
    */
  test("the generated corpus reaches nesting, nulls and non-ASCII text") {
    val values = Vector.fill(300)(anyJson.sample).flatten
    assert(values.sizeIs > 0, "no JSON was generated")
    val texts = values.map(CanonicalJson.print)
    assert(texts.exists(_.contains("null")), "no generated value carried an explicit null")
    assert(texts.exists(_.contains("ünïcode")), "no generated value carried non-ASCII text")
    assert(
      values.exists(_.asObject.exists(_.values.exists(_.isObject))),
      "no generated value nested an object inside an object, so depth is untested"
    )
  }
