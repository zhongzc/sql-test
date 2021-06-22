import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.util.{Await, Future, FuturePool}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Random.{alphanumeric, between, nextInt}

object Main extends App {
  val logger = LoggerFactory.getLogger(Main.getClass)
  val monitor = Monitor.startMonitoring(5000)
  val tableCount = 100
  val tuplesPerTable = 100000
  var client: () => Client = () =>
    Mysql.client
      .withCredentials("root", null)
      .withDatabase("test")
      .newRichClient("localhost:4000")

  try {
    Await.result(
      for {
        _ <- createTables(1).foreach(_ => logger.info("CREATE TABLES DONE!"))
        _ <- insertTuples(10).foreach(_ => logger.info("INSERT TUPLES DONE!"))
        _ <- randomPointGet(10).foreach(_ => logger.info("QUERY DONE!"))

        // _ <- other steps...
      } yield ()
    )
  } finally {
    monitor.stop()
  }

  import Monitor.MonitoredFuture

  def createTables(parallel: Int): Future[Unit] = {
    val futures = for {
      chunk <- (1 to tableCount).grouped(tableCount / parallel).toSeq
    } yield {
      val cli = client()
      cli
        .session(sess => {
          Future.traverseSequentially(chunk) { tableID =>
            for {
              _ <- sess
                .query(s"DROP TABLE IF EXISTS t_$tableID;")
                .monBy(monitor)
              _ <-
                sess
                  .query(
                    s"CREATE TABLE t_$tableID (" +
                      "a INT AUTO_INCREMENT," +
                      "b INT," +
                      "c VARCHAR(100)," +
                      "PRIMARY KEY (a)" +
                      ");"
                  )
                  .monBy(monitor)
            } yield ()
          }
        })
        .flatMap(_ => cli.close)
    }

    Future.join(futures)
  }

  def insertTuples(parallel: Int): Future[Unit] = {
    val batchSize = 200
    val batchSizes =
      (1 to tuplesPerTable).grouped(batchSize).map(_.length).toSeq
    val chunks = (1 to tableCount).grouped(tableCount / parallel).toSeq

    val futures = for {
      tableIDs <- chunks
    } yield {
      val cli = client()
      cli
        .session(sess => {
          Future.traverseSequentially(
            for {
              tableID <- tableIDs
              batchLen <- batchSizes
            } yield (tableID, batchLen)
          ) { case (tableID, batchLen) =>
            sess
              .query(
                Seq
                  .fill(batchLen)(
                    s"($nextInt, '${alphanumeric.take(between(5, 20)).mkString}')"
                  )
                  .mkString(s"INSERT INTO t_$tableID (b, c) VALUES ", ",", ";")
              )
              .monBy(monitor)
          }
        })
        .flatMap(_ => cli.close)
    }

    Future.join(futures)
  }

  def randomPointGet(parallel: Int): Future[Unit] = {
    val stopSignal = new AtomicBoolean(false)
    new Thread {
      override def run(): Unit = {
        Console.in.read
        stopSignal.set(true)
      }
    }.start()
    Future.join(Seq.fill(parallel) {
      val cli = client()
      cli
        .session(sess => {
          Future.whileDo(!stopSignal.get()) {
            sess
              .query(
                s"SELECT * FROM t_${between(1, tableCount + 1)} WHERE a = ${between(1, tuplesPerTable + 1)};"
              )
              .monBy(monitor)
          }
        })
        .flatMap(_ => cli.close)
    })
  }
}
