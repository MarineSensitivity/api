# API

The custom Application Programming Interface (API) using the R package  [`plumber`](https://www.rplumber.io/).

## start

Run the API in the background from server's RStudio **Terminal**:

```bash
# run as root and send to background (&)
sudo Rscript /share/github/MarineSensitivities/api/run-api.R &
```

## stop

```bash
# get the process id of the running service
ps -eaf | grep run-api
# root        2343    2254  0 16:58 pts/0    00:00:00 sudo Rscript /share/github/MarineSensitivities/api/run-api.R
# admin       2378    2254  0 17:00 pts/0    00:00:00 grep --color=auto run-api
sudo kill -9 2343
# [1]+  Killed                  Rscript /share/github/api/run-api.R
```

