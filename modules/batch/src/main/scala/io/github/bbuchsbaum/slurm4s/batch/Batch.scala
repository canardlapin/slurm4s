package io.github.bbuchsbaum.slurm4s.batch

import io.github.bbuchsbaum.slurm4s.core.*

import cats.Order
import cats.Show
import cats.data.NonEmptyChain
import cats.data.NonEmptyVector
import cats.syntax.all.*

import scala.compiletime.summonInline
import scala.deriving.Mirror

/** One deterministic axis in a parameter grid. */
final case class Axis[A] private (values: NonEmptyVector[A]) derives CanEqual

object Axis:
  def of[A](head: A, tail: A*): Axis[A] =
    Axis(NonEmptyVector(head, tail.toVector))

/** A non-empty, deterministically ordered collection of task inputs. */
final case class Grid[A] private (rows: NonEmptyVector[A]) derives CanEqual:
  /** `Long`, because a product of two large grids overflows `Int` long before it OOMs. */
  def size: Long = rows.length

  def map[B](f: A => B): Grid[B] =
    Grid(rows.map(f))

  /** Cross two grids, refusing a product too large to materialize.
    *
    * The product was previously built eagerly with an `Int` size: two 100k-row grids produced a
    * 10^10-element `flatMap` that exhausted the heap with no typed failure, while the row count
    * itself had none of the overflow guarding already applied to resources.
    */
  def product[B](other: Grid[B]): Either[BatchPlanFailure, Grid[(A, B)]] =
    val total = size * other.size
    if total > Grid.MaximumRows then
      Left(
        BatchPlanFailure.TooManyElements(
          s"a grid product of $total rows exceeds the maximum of ${Grid.MaximumRows}"
        )
      )
    else Right(Grid(rows.flatMap(left => other.rows.map(right => left -> right))))

object Grid:
  /** Bounds a materialized grid; a product beyond this is refused rather than attempted. */
  val MaximumRows: Long = 1_000_000L

  def one[A](value: A): Grid[A] =
    Grid(NonEmptyVector.one(value))

  def fromAxis[A](axis: Axis[A]): Grid[A] =
    Grid(axis.values)

  // Crossing axes is where a grid becomes large, so these carry the product's refusal outward
  // rather than hiding it behind an eager materialization.
  def cross[A, B, Z](first: Axis[A], second: Axis[B])(
      f: (A, B) => Z
  ): Either[BatchPlanFailure, Grid[Z]] =
    fromAxis(first).product(fromAxis(second)).map(_.map(f.tupled))

  def cross[A, B, C, Z](
      first: Axis[A],
      second: Axis[B],
      third: Axis[C]
  )(f: (A, B, C) => Z): Either[BatchPlanFailure, Grid[Z]] =
    for
      pair <- fromAxis(first).product(fromAxis(second))
      triple <- pair.product(fromAxis(third))
    yield triple.map { case ((a, b), c) => f(a, b, c) }

object Argument:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("argument", "must not be null"))
    else if raw.indexOf('\u0000') >= 0 then
      Left(ValidationFailure("argument", "must not contain NUL"))
    else Right(raw)

  private[batch] def trusted(raw: String): Type = raw

  extension (argument: Type) def value: String = argument
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

type Argument = Argument.Type

object ArgumentName:
  opaque type Type = String

  private val Supported = "[A-Za-z_][A-Za-z0-9_-]*".r

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("argumentName", "must not be null"))
    else if !Supported.matches(raw) then
      Left(
        ValidationFailure(
          "argumentName",
          "must use portable option-name syntax [A-Za-z_][A-Za-z0-9_-]*"
        )
      )
    else Right(raw)

  extension (name: Type)
    def value: String = name
    def option: Argument = Argument.trusted(s"--$name")
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

type ArgumentName = ArgumentName.Type

/** Encoding of one value into one process argument. */
trait ArgValue[-A]:
  def encode(value: A): Either[ValidationFailure, Argument]

object ArgValue:
  def instance[A](
      encodeValue: A => Either[ValidationFailure, Argument]
  ): ArgValue[A] =
    new ArgValue[A]:
      def encode(value: A): Either[ValidationFailure, Argument] = encodeValue(value)

  def fromString[A](render: A => String): ArgValue[A] =
    instance(value => Argument.from(render(value)))

  given ArgValue[String] = fromString(identity)
  given ArgValue[Int] = fromString(_.toString)
  given ArgValue[Long] = fromString(_.toString)
  given ArgValue[Double] = fromString(java.lang.Double.toString)
  given ArgValue[BigDecimal] = fromString(_.bigDecimal.toPlainString)
  given ArgValue[Boolean] = fromString(_.toString)

  inline def derived[A <: reflect.Enum]: ArgValue[A] =
    fromString(_.toString)

/** Converts one typed input into an exact argv suffix.
  *
  * Derivation for a case class emits `--field-name value` pairs in constructor order. The resulting
  * arguments are materialized before transport; this typeclass is never serialized.
  */
trait ScriptArguments[-A]:
  def encode(value: A): Either[NonEmptyChain[ValidationFailure], Vector[Argument]]

object ScriptArguments:
  def instance[A](
      encodeArguments: A => Either[NonEmptyChain[ValidationFailure], Vector[Argument]]
  ): ScriptArguments[A] =
    new ScriptArguments[A]:
      def encode(value: A): Either[NonEmptyChain[ValidationFailure], Vector[Argument]] =
        encodeArguments(value)

  def positional[A](using value: ArgValue[A]): ScriptArguments[A] =
    instance(input => value.encode(input).leftMap(NonEmptyChain.one).map(Vector(_)))

  def named[A](
      name: ArgumentName,
      get: A => String
  ): ScriptArguments[A] =
    instance { input =>
      Argument
        .from(get(input))
        .leftMap(NonEmptyChain.one)
        .map(value => Vector(name.option, value))
    }

  inline def derived[A <: Product](using mirror: Mirror.ProductOf[A]): ScriptArguments[A] =
    val encoder =
      summonInline[DerivedArguments[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
    instance { input =>
      encoder.encode(Tuple.fromProductTyped(input))
    }

  /** Compiler-facing support for `ScriptArguments.derived`. */
  sealed trait DerivedArguments[Labels <: Tuple, Values <: Tuple]:
    def encode(
        values: Values
    ): Either[NonEmptyChain[ValidationFailure], Vector[Argument]]

  given derivedEmpty: DerivedArguments[EmptyTuple, EmptyTuple] with
    def encode(
        values: EmptyTuple
    ): Either[NonEmptyChain[ValidationFailure], Vector[Argument]] =
      Right(Vector.empty)

  given derivedCons[
      Label <: String & Singleton,
      Labels <: Tuple,
      Value,
      Values <: Tuple
  ](using
      label: ValueOf[Label],
      valueEncoder: ArgValue[Value],
      tailEncoder: DerivedArguments[Labels, Values]
  ): DerivedArguments[Label *: Labels, Value *: Values] with
    def encode(
        values: Value *: Values
    ): Either[NonEmptyChain[ValidationFailure], Vector[Argument]] =
      val name = ArgumentName.from(label.value)
      val encoded = valueEncoder.encode(values.head)
      val remaining = tailEncoder.encode(values.tail)
      (
        name.toValidatedNec,
        encoded.toValidatedNec,
        remaining.toValidated
      ).mapN { (argumentName, argumentValue, rest) =>
        argumentName.option +: argumentValue +: rest
      }.toEither

/** A validated, non-empty command prefix such as `/usr/bin/env Rscript`. */
final case class CommandPrefix private (arguments: NonEmptyVector[Argument]) derives CanEqual

object CommandPrefix:
  def from(
      head: String,
      tail: String*
  ): Either[NonEmptyChain[ValidationFailure], CommandPrefix] =
    NonEmptyVector(head, tail.toVector)
      .traverse(Argument.from)
      .leftMap(NonEmptyChain.one)
      .map(CommandPrefix.apply)

  val bash: CommandPrefix =
    CommandPrefix(NonEmptyVector.one(Argument.trusted("/bin/bash")))

  val python3: CommandPrefix =
    CommandPrefix(
      NonEmptyVector.of(
        Argument.trusted("/usr/bin/env"),
        Argument.trusted("python3")
      )
    )

  val rscript: CommandPrefix =
    CommandPrefix(
      NonEmptyVector.of(
        Argument.trusted("/usr/bin/env"),
        Argument.trusted("Rscript")
      )
    )

enum ScriptInvocation derives CanEqual:
  case Direct
  case Via(prefix: CommandPrefix)

final case class ScriptProgram(
    source: ScriptSource,
    invocation: ScriptInvocation
) derives CanEqual

sealed trait BatchTask[I, O]

object BatchTask:
  final case class Script[I](
      program: ScriptProgram,
      arguments: ScriptArguments[I]
  ) extends BatchTask[I, NoResult]

  final case class Registered[I, O](
      operation: OperationRef[I, O],
      inputCodec: InputCodec[I],
      outputCodec: ResultCodec[O],
      maximumResultBytes: ByteLimit,
      retrySafety: RetrySafety
  ) extends BatchTask[I, O]

final case class Batch[I, O](
    grid: Grid[I],
    task: BatchTask[I, O]
)

/** Canonical durable identity for one logical element of a batch. */
object BatchElementKey:
  def derive(
      batch: SubmissionKey,
      index: ArrayIndex
  ): Either[ValidationFailure, SubmissionKey] =
    SubmissionKey.from(s"${batch.value}.element-${index.value}")

final case class TaskResources(
    cpus: PositiveInt,
    memory: Option[MemoryRequest],
    wallTime: Option[WallTimeMinutes]
) derives CanEqual

enum ShardAssignment derives CanEqual:
  case BalancedContiguous
  case RoundRobin
  case Exactly(rowsPerShard: PositiveInt)

enum BatchExecutionPlan derives CanEqual:
  case Independent(maximumRunning: Option[PositiveInt] = None)
  case Sharded(
      shards: PositiveInt,
      slotsPerShard: PositiveInt,
      assignment: ShardAssignment = ShardAssignment.BalancedContiguous,
      maximumRunningShards: Option[PositiveInt] = None
  )
  case Gang(nodes: PositiveInt, tasksPerNode: PositiveInt)

enum BatchPlanFailure derives CanEqual:
  case ConcurrencyExceedsTasks(requested: Int, tasks: Int)
  case ShardsExceedTasks(shards: Int, tasks: Int)
  case ExactAssignmentMismatch(shards: Int, rowsPerShard: Int, tasks: Int)
  case SlotsExceedLargestShard(slots: Int, largestShard: Int)
  case GangShapeMismatch(nodes: Int, tasksPerNode: Int, tasks: Int)
  case ResourceOverflow(field: String, left: Int, right: Int)
  case MemoryOverflow(mebibytes: Long, multiplier: Int)
  case InvalidElementIndex(index: Int)
  case EmptyShard(index: Int)
  case InvalidArray(failures: NonEmptyChain[ArrayPlanFailure])
  case TooManyElements(detail: String)

final case class PlannedBatchElement[I](
    index: ArrayIndex,
    input: I
) derives CanEqual

final case class BatchShard[I](
    index: ArrayIndex,
    elements: NonEmptyVector[PlannedBatchElement[I]]
) derives CanEqual

final case class BatchTopologyShard(
    index: ArrayIndex,
    elements: NonEmptyVector[ArrayIndex]
) derives CanEqual

final case class BatchTopology(
    execution: BatchExecutionPlan,
    resources: ResourceRequest,
    array: Option[JobArrayRequest],
    shards: NonEmptyVector[BatchTopologyShard]
) derives CanEqual:
  def elementIndices: NonEmptyVector[ArrayIndex] =
    shards.flatMap(_.elements).sorted

final case class BatchPlan[I, O] private[batch] (
    task: BatchTask[I, O],
    execution: BatchExecutionPlan,
    resources: ResourceRequest,
    array: Option[JobArrayRequest],
    shards: NonEmptyVector[BatchShard[I]]
):
  def elements: NonEmptyVector[PlannedBatchElement[I]] =
    shards.flatMap(_.elements).sortBy(_.index)

  def topology: BatchTopology =
    BatchTopology(
      execution,
      resources,
      array,
      shards.map(shard => BatchTopologyShard(shard.index, shard.elements.map(_.index)))
    )

object BatchPlanner:
  def compile[I, O](
      batch: Batch[I, O],
      execution: BatchExecutionPlan,
      perTask: TaskResources
  ): Either[NonEmptyChain[BatchPlanFailure], BatchPlan[I, O]] =
    indexed(batch.grid).flatMap { elements =>
      execution match
        case independent: BatchExecutionPlan.Independent =>
          compileIndependent(batch.task, elements, independent, perTask)
        case sharded: BatchExecutionPlan.Sharded =>
          compileSharded(batch.task, elements, sharded, perTask)
        case gang: BatchExecutionPlan.Gang =>
          compileGang(batch.task, elements, gang, perTask)
    }

  private def indexed[I](
      grid: Grid[I]
  ): Either[NonEmptyChain[BatchPlanFailure], NonEmptyVector[PlannedBatchElement[I]]] =
    grid.rows.zipWithIndex.traverse { case (input, offset) =>
      val index = offset + 1
      ArrayIndex
        .from(index)
        .leftMap(_ => NonEmptyChain.one(BatchPlanFailure.InvalidElementIndex(index)))
        .map(PlannedBatchElement(_, input))
    }

  private def compileIndependent[I, O](
      task: BatchTask[I, O],
      elements: NonEmptyVector[PlannedBatchElement[I]],
      execution: BatchExecutionPlan.Independent,
      perTask: TaskResources
  ): Either[NonEmptyChain[BatchPlanFailure], BatchPlan[I, O]] =
    val failures = execution.maximumRunning.toVector.collect {
      case maximum if maximum.toInt > elements.length =>
        BatchPlanFailure.ConcurrencyExceedsTasks(maximum.toInt, elements.length)
    }
    NonEmptyChain.fromSeq(failures) match
      case Some(values) => Left(values)
      case None         =>
        for
          resources <- resources(perTask, slots = 1, tasks = 1, nodes = 1)
          array <- array(
            elements.toVector.map(_.index),
            execution.maximumRunning
          )
        yield BatchPlan(
          task,
          execution,
          resources,
          Some(array),
          elements.map(element => BatchShard(element.index, NonEmptyVector.one(element)))
        )

  private def compileSharded[I, O](
      task: BatchTask[I, O],
      elements: NonEmptyVector[PlannedBatchElement[I]],
      execution: BatchExecutionPlan.Sharded,
      perTask: TaskResources
  ): Either[NonEmptyChain[BatchPlanFailure], BatchPlan[I, O]] =
    val shardCount = execution.shards.toInt
    val taskCount = elements.length
    val shapeFailures = Vector(
      Option.when(shardCount > taskCount)(
        BatchPlanFailure.ShardsExceedTasks(shardCount, taskCount)
      ),
      execution.assignment match
        case ShardAssignment.Exactly(rowsPerShard)
            if shardCount.toLong * rowsPerShard.toInt.toLong != taskCount.toLong =>
          Some(
            BatchPlanFailure.ExactAssignmentMismatch(
              shardCount,
              rowsPerShard.toInt,
              taskCount
            )
          )
        case _ => None,
      execution.maximumRunningShards.collect {
        case maximum if maximum.toInt > shardCount =>
          BatchPlanFailure.ConcurrencyExceedsTasks(maximum.toInt, shardCount)
      }
    ).flatten

    NonEmptyChain.fromSeq(shapeFailures) match
      case Some(values) => Left(values)
      case None         =>
        assign(elements, shardCount, execution.assignment).flatMap { assigned =>
          val largest = assigned.toVector.map(_.elements.length).max
          if execution.slotsPerShard.toInt > largest then
            Left(
              NonEmptyChain.one(
                BatchPlanFailure.SlotsExceedLargestShard(
                  execution.slotsPerShard.toInt,
                  largest
                )
              )
            )
          else
            for
              resources <- resources(
                perTask,
                execution.slotsPerShard.toInt,
                tasks = 1,
                nodes = 1
              )
              array <- array(
                assigned.toVector.map(_.index),
                execution.maximumRunningShards
              )
            yield BatchPlan(task, execution, resources, Some(array), assigned)
        }

  private def compileGang[I, O](
      task: BatchTask[I, O],
      elements: NonEmptyVector[PlannedBatchElement[I]],
      execution: BatchExecutionPlan.Gang,
      perTask: TaskResources
  ): Either[NonEmptyChain[BatchPlanFailure], BatchPlan[I, O]] =
    multiply("gangTasks", execution.nodes.toInt, execution.tasksPerNode.toInt).flatMap { taskCount =>
      if taskCount != elements.length then
        Left(
          NonEmptyChain.one(
            BatchPlanFailure.GangShapeMismatch(
              execution.nodes.toInt,
              execution.tasksPerNode.toInt,
              elements.length
            )
          )
        )
      else
        resources(
          perTask,
          slots = execution.tasksPerNode.toInt,
          tasks = taskCount,
          nodes = execution.nodes.toInt
        ).map { requested =>
          BatchPlan(
            task,
            execution,
            requested,
            None,
            NonEmptyVector.one(
              BatchShard(elements.head.index, elements)
            )
          )
        }
    }

  private def assign[I](
      elements: NonEmptyVector[PlannedBatchElement[I]],
      shardCount: Int,
      assignment: ShardAssignment
  ): Either[NonEmptyChain[BatchPlanFailure], NonEmptyVector[BatchShard[I]]] =
    val buckets = assignment match
      case ShardAssignment.RoundRobin =>
        elements.toVector.zipWithIndex
          .groupMap { case (_, offset) => offset % shardCount }(_._1)
      case ShardAssignment.BalancedContiguous | ShardAssignment.Exactly(_) =>
        val base = elements.length / shardCount
        val remainder = elements.length % shardCount
        val sizes = Vector.tabulate(shardCount) { shard =>
          base + Option.when(shard < remainder)(1).getOrElse(0)
        }
        sizes
          .foldLeft((0, Map.empty[Int, Vector[PlannedBatchElement[I]]])) {
            case ((offset, current), size) =>
              val next = elements.toVector.slice(offset, offset + size)
              (offset + size, current.updated(current.size, next))
          }
          ._2

    NonEmptyVector
      .of(0, (1 until shardCount)*)
      .traverse { offset =>
        val index = offset + 1
        for
          shardIndex <- ArrayIndex
            .from(index)
            .leftMap(_ => NonEmptyChain.one(BatchPlanFailure.InvalidElementIndex(index)))
          shardElements <- NonEmptyVector
            .fromVector(buckets(offset))
            .toRight(NonEmptyChain.one(BatchPlanFailure.EmptyShard(index)))
        yield BatchShard(
          shardIndex,
          shardElements
        )
      }

  private def resources(
      perTask: TaskResources,
      slots: Int,
      tasks: Int,
      nodes: Int
  ): Either[NonEmptyChain[BatchPlanFailure], ResourceRequest] =
    for
      // A single-task allocation owns every slot's CPUs, so the per-task request is the product.
      // A multi-task allocation states CPUs per task directly, and the product is never requested;
      // computing it there would let an unused quantity overflow an otherwise legal shape.
      cpus <-
        if tasks == 1 then multiply("cpusPerTask", perTask.cpus.toInt, slots)
        else Right(perTask.cpus.toInt)
      cpusValue <- PositiveInt
        .from("cpusPerTask", cpus)
        .leftMap(_ =>
          NonEmptyChain.one(
            BatchPlanFailure.ResourceOverflow("cpusPerTask", perTask.cpus.toInt, slots)
          )
        )
      tasksValue <- PositiveInt
        .from("tasks", tasks)
        .leftMap(_ => NonEmptyChain.one(BatchPlanFailure.ResourceOverflow("tasks", tasks, 1)))
      nodesValue <- PositiveInt
        .from("nodes", nodes)
        .leftMap(_ => NonEmptyChain.one(BatchPlanFailure.ResourceOverflow("nodes", nodes, 1)))
      memory <- memory(perTask.memory, slots)
    yield ResourceRequest(
      cpusValue,
      tasksValue,
      Some(nodesValue),
      memory,
      perTask.wallTime
    )

  private def memory(
      request: Option[MemoryRequest],
      multiplier: Int
  ): Either[NonEmptyChain[BatchPlanFailure], Option[MemoryRequest]] =
    request.traverse {
      case MemoryRequest.PerNode(amount) =>
        val raw = amount.toLong
        Either
          .cond(
            raw <= Long.MaxValue / multiplier.toLong,
            raw * multiplier.toLong,
            NonEmptyChain.one(BatchPlanFailure.MemoryOverflow(raw, multiplier))
          )
          .flatMap { total =>
            Mebibytes
              .from(total)
              .leftMap(_ => NonEmptyChain.one(BatchPlanFailure.MemoryOverflow(raw, multiplier)))
          }
          .map(MemoryRequest.PerNode.apply)
      case perCpu: MemoryRequest.PerCpu => Right(perCpu)
      case MemoryRequest.AllNodeMemory  => Right(MemoryRequest.AllNodeMemory)
    }

  private def multiply(
      field: String,
      left: Int,
      right: Int
  ): Either[NonEmptyChain[BatchPlanFailure], Int] =
    val result = left.toLong * right.toLong
    Either.cond(
      result <= Int.MaxValue.toLong,
      result.toInt,
      NonEmptyChain.one(BatchPlanFailure.ResourceOverflow(field, left, right))
    )

  private def array(
      indices: Vector[ArrayIndex],
      maximumConcurrent: Option[PositiveInt]
  ): Either[NonEmptyChain[BatchPlanFailure], JobArrayRequest] =
    JobArrayRequest
      .from(indices, maximumConcurrent)
      .leftMap(failures => NonEmptyChain.one(BatchPlanFailure.InvalidArray(failures)))
