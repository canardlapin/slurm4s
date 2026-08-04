package io.github.bbuchsbaum.remoteexec.kernel

import scodec.bits.ByteVector

/** Strict equality for owned byte storage.
  *
  * `ByteVector` is a third-party type, so `strictEquality` cannot find a `CanEqual` in its
  * companion the way it does for every type in this codebase that derives one. Declaring it once
  * here keeps byte comparison available to every module without each restating it, and without
  * relaxing strict equality for anything else. Import it as
  * `io.github.bbuchsbaum.remoteexec.kernel.byteVectorCanEqual`.
  */
given byteVectorCanEqual: CanEqual[ByteVector, ByteVector] = CanEqual.derived
