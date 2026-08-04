package io.github.bbuchsbaum.slurm4s.core

class SharedKernelFacadeSuite extends munit.FunSuite:
  test("core re-exports preserve exact kernel type identity") {
    val throughCore: SubmissionKey = SubmissionKey.from("facade-key").toOption.get
    val throughKernel: io.github.bbuchsbaum.remoteexec.kernel.SubmissionKey = throughCore
    val backThroughCore: SubmissionKey = throughKernel

    assertEquals(backThroughCore.value, "facade-key")
  }

  test("core companion aliases construct the kernel-owned values") {
    val limit: ByteLimit = ByteLimit.from(4096).toOption.get
    val kernelLimit: io.github.bbuchsbaum.remoteexec.kernel.ByteLimit = limit
    val diagnostic: Diagnostic = Diagnostic("facade", "same value")
    val kernelDiagnostic: io.github.bbuchsbaum.remoteexec.kernel.Diagnostic = diagnostic

    assertEquals(kernelLimit.value, 4096)
    assertEquals(kernelDiagnostic.code, "facade")
  }
