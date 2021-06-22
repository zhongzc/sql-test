import com.twitter.util.Future
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{LinkedTransferQueue, TimeUnit}

class Monitor {
  val opCount = new AtomicLong(0)
  val channel = new LinkedTransferQueue[(Long, Long)]
  val stopNotifier = new AtomicBoolean(false)
  var thread: Thread = _

  def incrCount(count: Long): Unit = {
    opCount.addAndGet(count)
  }

  def addResponseTime(nanos: Long, count: Long): Unit = {
    channel.put((nanos, count))
  }

  def stop(): Unit = {
    stopNotifier.set(true)
    thread.join()
  }
}

object Monitor {
  def startMonitoring(reportIntervalMillis: Long): Monitor = {
    val monitor = new Monitor
    val thread = new Thread {
      override def run(): Unit = {
        val logger = LoggerFactory.getLogger(Monitor.getClass)
        val histogram = new Histogram(3600000000000L, 3)
        var now = System.currentTimeMillis()
        var nextReportTime = now + reportIntervalMillis
        var lastCount = 0L

        while (!monitor.stopNotifier.get()) {
          now = System.currentTimeMillis()
          if (now > nextReportTime) {
            val count = monitor.opCount.get()
            val qps =
              (count - lastCount).toDouble / (reportIntervalMillis.toDouble / 1000)
            val p50 = histogram.getValueAtPercentile(50.0).toDouble / 1000000
            val p90 = histogram.getValueAtPercentile(90.0).toDouble / 1000000
            val p99 = histogram.getValueAtPercentile(99.0).toDouble / 1000000
            val p999 = histogram.getValueAtPercentile(99.9).toDouble / 1000000
            val min = histogram.getMinValue.toDouble / 1000000
            val avg = histogram.getMean / 1000000
            val max = histogram.getMaxValue.toDouble / 1000000
            val stddev = histogram.getStdDeviation / 1000000

            logger.info(
              f"QPS: $qps%.2f | Latency (ms): p50 $p50%.2f, p90 $p90%.2f, p99 $p99%.2f, p999 $p999%.2f, Min $min%.2f, Avg $avg%.2f, Max $max%.2f, Stddev $stddev%.2f"
            )

            lastCount = count
            nextReportTime = now + reportIntervalMillis
            histogram.reset()
          }

          val pair =
            monitor.channel.poll(nextReportTime - now, TimeUnit.MILLISECONDS)
          if (pair != null) {
            histogram.recordValueWithCount(pair._1, pair._2)
          }
        }
      }
    }
    monitor.thread = thread
    thread.start()
    monitor
  }

  implicit class MonitoredFuture[A](future: Future[A]) {
    def monBy(monitor: Monitor): Future[A] = future.monBy(monitor, 1)

    def monBy(monitor: Monitor, count: Long): Future[A] = {
      for {
        startNanos <- {
          monitor.incrCount(count)
          Future.value(System.nanoTime())
        }
        v <- future
        _ <- {
          monitor.addResponseTime(System.nanoTime() - startNanos, count)
          Future.value(())
        }
      } yield v
    }
  }
}
