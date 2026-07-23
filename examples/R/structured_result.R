# Minimal R producer for scala-slurm result envelope v1.
# Requires jsonlite and openssl; these are workload helpers, not scala-slurm runtime dependencies.

sha256 <- function(bytes) {
  paste0("sha256:", as.character(openssl::sha256(bytes)))
}

output_entry <- function(root, relative) {
  path <- file.path(root, relative)
  size <- file.info(path)$size
  bytes <- readBin(path, what = "raw", n = size)
  list(path = relative, sizeBytes = size, digest = sha256(bytes))
}

encode_envelope <- function(payload) {
  canonicalize <- function(value) {
    if (is.list(value)) {
      value <- lapply(value, canonicalize)
      if (!is.null(names(value))) value <- value[sort(names(value))]
    }
    value
  }
  document <- list(
    protocol = list(major = 1L, minor = 0L),
    schema = "scala-slurm.result-envelope",
    payload = payload
  )
  text <- jsonlite::toJSON(canonicalize(document), auto_unbox = TRUE, null = "null", digits = NA)
  charToRaw(paste0(text, "\n"))
}

publish_atomic <- function(target, bytes) {
  if (file.exists(target)) stop(paste("result already exists:", target))
  temporary <- paste0(dirname(target), "/.", basename(target), ".tmp-", Sys.getpid())
  connection <- file(temporary, open = "wb")
  on.exit(try(close(connection), silent = TRUE), add = TRUE)
  writeBin(bytes, connection)
  close(connection)
  if (!file.rename(temporary, target)) stop("atomic result publication failed")
}

succeeded_payload <- function(submission_key, attempt_id, attempt_epoch, operation,
                              result_schema, value, outputs, worker_release, completed_at) {
  list(
    submissionKey = submission_key,
    attemptId = attempt_id,
    attemptEpoch = attempt_epoch,
    job = NULL,
    operation = operation,
    resultSchema = result_schema,
    status = list(kind = "succeeded"),
    valueBase64 = jsonlite::base64_enc(value),
    outputs = outputs,
    workerRelease = worker_release,
    completedAt = completed_at
  )
}
