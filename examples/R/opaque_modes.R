# Tiny opaque R workload demonstrating exit-only, declared-output, and structured modes.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L) stop("usage: Rscript opaque_modes.R MODE WORKSPACE")
mode <- args[[1L]]
workspace <- args[[2L]]
if (!mode %in% c("exit-only", "declared-output", "structured")) stop("unknown mode")
if (mode == "exit-only") quit(status = 0L)

output <- file.path(workspace, "results", "answer.txt")
dir.create(dirname(output), recursive = TRUE, showWarnings = FALSE)
writeLines("42", output, useBytes = TRUE)
if (mode == "declared-output") quit(status = 0L)

script_arg <- commandArgs(trailingOnly = FALSE)
script_file <- sub("^--file=", "", script_arg[grepl("^--file=", script_arg)][[1L]])
source(file.path(dirname(normalizePath(script_file)), "structured_result.R"))
payload <- succeeded_payload(
  "r-opaque-example",
  "r-opaque-attempt",
  1L,
  list(kind = "script", sourceDigest = paste0("sha256:", strrep("c", 64L))),
  "example.answer.v1",
  charToRaw('{"answer":42}'),
  list(output_entry(workspace, "results/answer.txt")),
  list(id = "r-helper-1", digest = paste0("sha256:", strrep("d", 64L))),
  "2026-07-22T12:00:00Z"
)
publish_atomic(file.path(workspace, "result.json"), encode_envelope(payload))
