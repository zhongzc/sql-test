# MySQL Test Script

```shell
$ sbt run
[info] welcome to sbt 1.5.4 (Ubuntu Java 11.0.11)
...
[info] running Main
Jun 19, 2021 7:38:33 PM com.twitter.finagle.Init$ $anonfun$once$1
INFO: Finagle version 21.5.0 (rev=91ff887d297f5d7b46dfec703fa6486a45b18b9b) built at 20210529-011844
Jun 19, 2021 7:38:33 PM com.twitter.finagle.BaseResolver inetResolver$lzycompute
INFO: Using default inet resolver
2021-06-19 19:38:37:140 +0800 INFO Monitor$ - QPS: 3.80 | Latency (ms): p50 111.87, p90 255.98, p99 301.47, p999 301.47, Min 103.28, Avg 182.68, Max 301.47, Stddev 75.69
2021-06-19 19:38:42:128 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.97, p90 252.97, p99 255.33, p999 255.33, Min 100.73, Avg 178.15, Max 255.33, Stddev 71.87
2021-06-19 19:38:47:124 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.58, p90 253.23, p99 255.98, p999 255.98, Min 99.94, Avg 179.14, Max 255.98, Stddev 72.15
2021-06-19 19:38:52:123 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.05, p90 252.44, p99 256.64, p999 256.64, Min 96.93, Avg 175.57, Max 256.64, Stddev 71.15
2021-06-19 19:38:57:123 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.31, p90 251.79, p99 254.41, p999 254.41, Min 100.14, Avg 177.04, Max 254.41, Stddev 71.48
2021-06-19 19:39:02:123 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.12, p90 253.10, p99 257.03, p999 257.03, Min 102.11, Avg 178.04, Max 257.03, Stddev 71.95
2021-06-19 19:39:07:124 +0800 INFO Monitor$ - QPS: 5.60 | Latency (ms): p50 109.12, p90 251.27, p99 252.05, p999 252.05, Min 100.99, Avg 176.68, Max 252.05, Stddev 71.12
[success] Total time: 45 s, completed Jun 19, 2021, 7:39:12 PM
```
