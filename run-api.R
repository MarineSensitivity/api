#!/usr/bin/env Rscript
library(plumber)

pr("/share/github/MarineSensitivities/api/plumber.R") |>
  pr_run(port=8888, debug=T, host="0.0.0.0")
