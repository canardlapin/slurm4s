package io.github.bbuchsbaum.remoteexec.kernel

/** The complete portable identity of an operation.
  *
  * This is the sole production value that stores the id/version/schema tuple. Executable
  * registrations and typed references wrap this descriptor rather than copying its fields.
  */
final case class OperationDescriptor(
    id: OperationId,
    version: OperationVersion,
    inputSchema: SchemaId,
    outputSchema: ResultSchemaId
) derives CanEqual:
  /** Compatibility spelling for callers that describe the output as a result. */
  def resultSchema: ResultSchemaId = outputSchema

/** A typed view of a portable [[OperationDescriptor]].
  *
  * `I` and `O` are compile-time witnesses only; the descriptor remains the complete wire identity.
  */
final case class OperationRef[I, O](descriptor: OperationDescriptor) derives CanEqual:
  def id: OperationId = descriptor.id
  def version: OperationVersion = descriptor.version
  def inputSchema: SchemaId = descriptor.inputSchema
  def outputSchema: ResultSchemaId = descriptor.outputSchema

object OperationRef:
  def apply[I, O](
      id: OperationId,
      version: OperationVersion,
      inputSchema: SchemaId,
      outputSchema: ResultSchemaId
  ): OperationRef[I, O] =
    OperationRef(OperationDescriptor(id, version, inputSchema, outputSchema))
