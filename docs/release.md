# Release procedure

slurm4s is pre-release software. No public compatibility baseline exists yet; P7.6 owns the first
`0.1` baseline, the first release tag, and proof that MiMa rejects a deliberate incompatible
change. Until that work lands, CI deliberately does not display a binary-compatibility step:
`mimaPreviousArtifacts` is empty, so such a step would check nothing.

## Local release gate

Use JDK 17 and start from a clean checkout. Set `JAVA_HOME` explicitly to the JDK that sbt must use;
the gate inspects that launcher and refuses any other specification version before running sbt.
Run:

```sh
tools/acceptance/v1-local-gate.sh
```

The gate checks the acceptance scripts, formatting, both supported Scala compiler lanes, local
publication of every publishable artifact, and the worker package. Scala 3.3.8 LTS is the sole
publication baseline; Scala 3.8.4 is verification-only, and its publish tasks are skipped.

Before tagging, also confirm:

```sh
sbt githubWorkflowCheck
git status --short
```

The generated workflow must be current and the tree must contain only the intended release
changes.

## CI publication

The sbt-typelevel workflow publishes snapshots from `main` and releases from tags matching `v*`
through `tlCiRelease`. It expects the repository's Sonatype and PGP secrets described by the
generated workflow. Do not create a release tag until those secrets and the target namespace have
been verified in the repository settings.

The first release additionally requires P7.6:

1. choose and record the exact `0.1` version and tag;
2. enable `tlCiMimaBinaryIssueCheck`;
3. verify that every published module has a non-empty MiMa baseline;
4. prove the gate fails for a deliberate incompatible public-API change, then revert that probe;
5. run the full local gate and the remote CI matrix before publishing the tag.

Live Slurm-site support evidence is a separate acceptance claim. A successful artifact publication
must not be reported as proof of scheduler or site compatibility.
