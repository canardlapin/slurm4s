package io.github.bbuchsbaum.slurm4s.core

import org.scalacheck.Prop.forAll

/** P8.B2 law family 1: attribution.
  *
  * A scheduler report belongs to an attempt exactly when their operational identities agree. These
  * laws pin the rule that P8.A1 established after whole-`JobRef` equality silently discarded every
  * managed observation on any site whose `squeue` reports a cluster.
  */
class AttributionLawSuite extends munit.ScalaCheckSuite:
  import Generators.*

  property("operational identity ignores the reported cluster") {
    forAll(jobRef, org.scalacheck.Gen.option(clusterName)) { (ref, cluster) =>
      ref.copy(cluster = cluster).key == ref.key
    }
  }

  property("operational identity distinguishes job number and array index") {
    forAll(jobRef, jobRef) { (left, right) =>
      (left.key == right.key) == (left.jobId == right.jobId && left.arrayIndex == right.arrayIndex)
    }
  }

  property("cluster conflict is symmetric") {
    forAll(jobRef, jobRef) { (left, right) =>
      left.clusterConflictsWith(right) == right.clusterConflictsWith(left)
    }
  }

  property("a job never conflicts with itself") {
    forAll(jobRef)(ref => !ref.clusterConflictsWith(ref))
  }

  property("an unknown cluster on either side is never a conflict") {
    forAll(jobRef, jobRef) { (left, right) =>
      val unknown = right.copy(cluster = None)
      !left.clusterConflictsWith(unknown) && !unknown.clusterConflictsWith(left)
    }
  }

  property("two known and different clusters always conflict") {
    forAll(jobRef, clusterName, clusterName) { (ref, left, right) =>
      val conflicts =
        ref.copy(cluster = Some(left)).clusterConflictsWith(ref.copy(cluster = Some(right)))
      conflicts == (left != right)
    }
  }
