import java.time.LocalDateTime
import java.util.concurrent.{LinkedTransferQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.util.{Await, Future}
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory

object Main extends App {
  import Monitor.MonitoredFuture

  val client = Mysql.client
    .withCredentials("root", null)
    .withDatabase("test")
    .newRichClient("localhost:4000")

  val monitor = Monitor.startMonitoring(5000)

  Await.result(
    for {
      _ <- createTables(client, monitor, 100, 1)
    } yield ()
  )
  monitor.stop()

  def createTables(client: Client, monitor: Monitor, count: Int, parallel: Int): Future[()] = {
    Future.collect(
      (1 to count)
        .grouped(count / parallel)
        .map(table_ids =>
          client.session(sess => {
            var f = Future.value(())
            for (table_id <- table_ids) {
              f = for {
                _ <- f
                _ <- sess.query(s"DROP TABLE IF EXISTS t_$table_id;").monBy(monitor)
                _ <- sess.query(
                  s"CREATE TABLE IF NOT EXISTS t_$table_id (a int AUTO_INCREMENT, b int, c varchar(100), primary key (a));"
                ).monBy(monitor)
              } yield ()
            }
            f
          })
        )
        .toSeq
    ).map(_ => ())
  }
}


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
            val qps = (count - lastCount).toDouble / (reportIntervalMillis.toDouble / 1000)
            val p50 = histogram.getValueAtPercentile(50.0).toDouble / 1000000
            val p90 = histogram.getValueAtPercentile(90.0).toDouble / 1000000
            val p99 = histogram.getValueAtPercentile(99.0).toDouble / 1000000
            val p999 = histogram.getValueAtPercentile(99.9).toDouble / 1000000
            val min = histogram.getMinValue.toDouble / 1000000
            val avg = histogram.getMean / 1000000
            val max = histogram.getMaxValue.toDouble / 1000000
            val stddev = histogram.getStdDeviation / 1000000

            logger.info(f"[${LocalDateTime.now()}] QPS: $qps%.2f | Latency (ms): p50 $p50%.2f, p90 $p90%.2f, p99 $p99%.2f, p999 $p999%.2f, Min $min%.2f, Avg $avg%.2f, Max $max%.2f, Stddev $stddev%.2f")

            lastCount = count
            nextReportTime = now + reportIntervalMillis
            histogram.reset()
          }

          val pair = monitor.channel.poll(nextReportTime - now, TimeUnit.MILLISECONDS)
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
    def monBy(monitor: Monitor): Future[A] = {
      for {
        startNanos <- {
          monitor.incrCount(1)
          Future.value(System.nanoTime())
        }
        v <- future
        _ <- {
          monitor.addResponseTime(System.nanoTime() - startNanos, 1)
          Future.value(())
        }
      } yield v
    }
  }
}
