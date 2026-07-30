package io.github.bbuchsbaum.slurm4s.core

import cats.data.NonEmptyChain
import munit.FunSuite

class BatchSuite extends FunSuite:
  enum Method derives CanEqual:
    case Ridge, Lasso, ElasticNet

  given ArgValue[Method] = ArgValue.fromString(_.toString.toLowerCase)

  final case class FitParams(alpha: Double, method: Method, seed: Int)

  given ScriptArguments[FitParams] = ScriptArguments.derived

  private val grid =
    Grid.cross(
      Axis.of(0.1, 0.5, 1.0),
      Axis.of(Method.Ridge, Method.Lasso, Method.ElasticNet),
      Axis.of(1, 2, 3)
    )(FitParams.apply)

  private val task =
    BatchTask.Script(
      ScriptProgram(
        ScriptSource.ExistingRemote("/work/fit.R"),
        ScriptInvocation.Via(CommandPrefix.rscript)
      ),
      summon[ScriptArguments[FitParams]]
    )

  private val resources =
    TaskResources(
      positive("cpus", 2),
      Some(MemoryRequest.PerNode(mebibytes(1024))),
      Some(WallTimeMinutes.from(60).toOption.get)
    )

  test("a crossed 3x3x3 grid is deterministic and derives named arguments") {
    assertEquals(grid.size, 27)
    assertEquals(
      grid.rows.head,
      FitParams(0.1, Method.Ridge, 1)
    )
    assertEquals(
      grid.rows.last,
      FitParams(1.0, Method.ElasticNet, 3)
    )

    val encoded = summon[ScriptArguments[FitParams]]
      .encode(grid.rows.head)
      .map(_.map(_.value))

    assertEquals(
      encoded,
      Right(
        Vector(
          "--alpha",
          "0.1",
          "--method",
          "ridge",
          "--seed",
          "1"
        )
      )
    )
  }

  test("independent execution produces a one-based 27-element array") {
    val plan = compile(BatchExecutionPlan.Independent())

    assertEquals(plan.array.map(_.indices.toVector.map(_.value)), Some((1 to 27).toVector))
    assertEquals(plan.shards.length, 27)
    assert(plan.shards.forall(_.elements.length == 1))
    assertEquals(plan.resources.cpusPerTask.toInt, 2)
    assertEquals(plan.resources.memory, Some(MemoryRequest.PerNode(mebibytes(1024))))
  }

  test("nine shards with three slots derive per-node resources and balanced rows") {
    val plan = compile(
      BatchExecutionPlan.Sharded(
        shards = positive("shards", 9),
        slotsPerShard = positive("slotsPerShard", 3)
      )
    )

    assertEquals(plan.shards.length, 9)
    assert(plan.shards.forall(_.elements.length == 3))
    assertEquals(plan.resources.tasks.toInt, 1)
    assertEquals(plan.resources.nodes.map(_.toInt), Some(1))
    assertEquals(plan.resources.cpusPerTask.toInt, 6)
    assertEquals(plan.resources.memory, Some(MemoryRequest.PerNode(mebibytes(3072))))
  }

  test("three shards assign nine rows each while allowing five slots") {
    val plan = compile(
      BatchExecutionPlan.Sharded(
        shards = positive("shards", 3),
        slotsPerShard = positive("slotsPerShard", 5),
        assignment = ShardAssignment.Exactly(positive("rowsPerShard", 9))
      )
    )

    assertEquals(plan.shards.map(_.elements.length).toVector, Vector(9, 9, 9))
    assertEquals(plan.resources.cpusPerTask.toInt, 10)
    assertEquals(plan.resources.memory, Some(MemoryRequest.PerNode(mebibytes(5120))))
  }

  test("gang execution validates and derives nine nodes with 27 ranks") {
    val plan = compile(
      BatchExecutionPlan.Gang(
        nodes = positive("nodes", 9),
        tasksPerNode = positive("tasksPerNode", 3)
      )
    )

    assertEquals(plan.array, None)
    assertEquals(plan.resources.nodes.map(_.toInt), Some(9))
    assertEquals(plan.resources.tasks.toInt, 27)
    assertEquals(plan.resources.cpusPerTask.toInt, 2)
    assertEquals(plan.resources.memory, Some(MemoryRequest.PerNode(mebibytes(3072))))
  }

  test("invalid exact and gang shapes remain typed planning failures") {
    val exact = BatchPlanner.compile(
      Batch(grid, task),
      BatchExecutionPlan.Sharded(
        positive("shards", 4),
        positive("slots", 2),
        ShardAssignment.Exactly(positive("rows", 7))
      ),
      resources
    )
    val gang = BatchPlanner.compile(
      Batch(grid, task),
      BatchExecutionPlan.Gang(positive("nodes", 3), positive("tasks", 8)),
      resources
    )

    assert(
      exact.left.exists(
        _.exists(_.isInstanceOf[BatchPlanFailure.ExactAssignmentMismatch])
      )
    )
    assert(
      gang.left.exists(
        _.exists(_.isInstanceOf[BatchPlanFailure.GangShapeMismatch])
      )
    )
  }

  private def compile(execution: BatchExecutionPlan): BatchPlan[FitParams, NoResult] =
    BatchPlanner
      .compile(Batch(grid, task), execution, resources)
      .fold(failures => fail(render(failures)), identity)

  private def render(failures: NonEmptyChain[BatchPlanFailure]): String =
    failures.toChain.toList.mkString(", ")

  private def positive(field: String, value: Int): PositiveInt =
    PositiveInt.from(field, value).fold(problem => fail(problem.reason), identity)

  private def mebibytes(value: Long): Mebibytes =
    Mebibytes.from(value).fold(problem => fail(problem.reason), identity)
