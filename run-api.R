#!/usr/bin/env Rscript
library(plumber)

#* @filter cors
cors <- function(res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  plumber::forward()
}

pr("/share/github/MarineSensitivity/api/plumber.R") |>
  cors() |>
  pr_run(port=8888, debug=T, host="0.0.0.0")
