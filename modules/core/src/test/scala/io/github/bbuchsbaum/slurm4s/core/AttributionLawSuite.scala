package io.github.bbuchsbaum.slurm4s.core

import org.scalacheck.Prop.forAll

/** P8.B2 law family 1: attribution, restated after P8.D4.
  *
  * A report belongs to an attempt exactly when their operational identities agree, and operational
  * identity is now base job id plus optional array element — nothing else. The laws that used to
  * describe cluster-conflict behaviour are gone because the field is gone; what remains is the
  * property those laws existed to protect.
  */
class AttributionLawSuite extends munit.ScalaCheckSuite:
  import Generators.*

  property("operational identity is exactly job number and array element") {
    forAll(jobRef, jobRef) { (left, right) =>
      (left == right) == (left.jobId == right.jobId && left.arrayIndex == right.arrayIndex)
    }
  }

  property("a job always attributes to itself") {
    forAll(jobRef)(ref => ref == ref)
  }

  property("array elements of one job are distinct identities") {
    forAll(jobId, arrayIndex, arrayIndex) { (id, left, right) =>
      val same = JobRef(id, Some(left)) == JobRef(id, Some(right))
      same == (left == right)
    }
  }

  property("an array element is never the same job as its parent") {
    forAll(jobId, arrayIndex) { (id, index) =>
      JobRef(id, Some(index)) != JobRef(id, None)
    }
  }

  /** The cluster a site reports must not reach identity by any route, which is what made every
    * managed observation vanish before D4.
    */
  property("a reported cluster does not participate in attribution") {
    forAll(jobRef, clusterName, evidenceBundle) { (ref, cluster, bundle) =>
      val observation = JobObservation(
        ref,
        SlurmState.Running,
        Freshness.Current(java.time.Instant.EPOCH),
        None,
        Map.empty,
        bundle,
        reportedCluster = Some(cluster)
      )
      observation.job == ref
    }
  }
