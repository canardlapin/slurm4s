package io.github.bbuchsbaum.slurm4s.core.codec

import io.circe.Json
import io.circe.Printer

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

/** The single canonical JSON rendering in this repository.
  *
  * Canonical means two peers that hold equal JSON produce equal bytes, which is what lets bytes
  * stand in for values: the control journal checksums a command's rendering to detect a corrupted
  * record, and a managed request's content digest is its identity, so the controller treats two
  * requests with the same digest as the same request.
  *
  * Both of those break silently if two renderings disagree — a checksum computed under one printer
  * and verified under another rejects a valid record, and a digest computed under one printer stops
  * matching an identical request recorded under the other. Neither failure names its cause. So this
  * is one definition rather than a convention that three call sites are trusted to repeat.
  *
  * The settings are the contract, not preferences:
  *   - `sortKeys` makes the rendering independent of field insertion order;
  *   - `dropNullValues = false` keeps an explicit null distinguishable from an absent field;
  *   - `noSpaces` removes the insignificant whitespace two encoders could otherwise differ on.
  */
object CanonicalJson:
  val printer: Printer = Printer.noSpaces.copy(dropNullValues = false, sortKeys = true)

  def print(json: Json): String = printer.print(json)

  def bytes(json: Json): ByteVector =
    ByteVector.view(print(json).getBytes(StandardCharsets.UTF_8))
