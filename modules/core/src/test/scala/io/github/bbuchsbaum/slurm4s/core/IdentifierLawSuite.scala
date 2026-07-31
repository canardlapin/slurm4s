package io.github.bbuchsbaum.slurm4s.core

import cats.kernel.laws.discipline.OrderTests
import munit.DisciplineSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Cogen

/** P8.B2: the `Order` instances on identifier companions are now verified, not asserted.
  *
  * Every identifier ships `CanEqual`, `Order`, `Ordering` and `Show`, and
  * `-language:strictEquality` makes those load-bearing rather than decorative — but no law had ever
  * been checked. An `Order` that is not transitive silently corrupts any sorted structure keyed by
  * these values.
  */
class IdentifierLawSuite extends DisciplineSuite:
  import Generators.*

  private given Arbitrary[JobId] = Arbitrary(jobId)
  private given Arbitrary[ClusterName] = Arbitrary(clusterName)
  private given Arbitrary[ArrayIndex] = Arbitrary(arrayIndex)

  private given Cogen[JobId] = Cogen.cogenString.contramap(_.value)
  private given Cogen[ClusterName] = Cogen.cogenString.contramap(_.value)
  private given Cogen[ArrayIndex] = Cogen.cogenInt.contramap(_.value)

  checkAll("Order[JobId]", OrderTests[JobId].order)
  checkAll("Order[ClusterName]", OrderTests[ClusterName].order)
  checkAll("Order[ArrayIndex]", OrderTests[ArrayIndex].order)
