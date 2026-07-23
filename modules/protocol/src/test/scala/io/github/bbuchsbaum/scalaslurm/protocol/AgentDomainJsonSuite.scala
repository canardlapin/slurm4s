package io.github.bbuchsbaum.scalaslurm.protocol

import io.github.bbuchsbaum.scalaslurm.core.*

class AgentDomainJsonSuite extends munit.FunSuite:
  test("agent submit JSON round-trips array identity and accepts legacy absence") {
    val array = JobArrayRequest
      .contiguous(
        PositiveInt.from("size", 3).toOption.get,
        Some(PositiveInt.from("maximumConcurrent", 2).toOption.get)
      )
      .toOption
      .get
    val request = JobRequest(
      SubmissionKey.from("wire-array").toOption.get,
      JobName.from("wire-array").toOption.get,
      Payload.Script(
        ScriptSource.ExistingRemote("/work/array.sh"),
        Vector.empty,
        ResultContract.ExitOnly
      ),
      ResourceRequest.validate(1, 1, None, None, None).toOption.get,
      Map.empty,
      Some(array)
    )

    val encoded = AgentDomainJson.encodeSubmitRequest(request).toOption.get
    assertEquals(AgentDomainJson.decodeSubmitRequest(encoded), Right(request))

    val legacy = encoded.mapObject(_.remove("array"))
    assertEquals(
      AgentDomainJson.decodeSubmitRequest(legacy).map(_.array),
      Right(None)
    )

    val ordinary = request.copy(array = None)
    val ordinaryJson = AgentDomainJson.encodeSubmitRequest(ordinary).toOption.get
    assert(!ordinaryJson.hcursor.downField("array").succeeded)
    assertEquals(AgentDomainJson.decodeSubmitRequest(ordinaryJson), Right(ordinary))
  }
