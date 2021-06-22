import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.util.{Await, Future}
import org.slf4j.LoggerFactory

import scala.util.Random.{alphanumeric, between, nextInt}

object Main extends App {
  val logger = LoggerFactory.getLogger(Main.getClass)
  val monitor = Monitor.startMonitoring(5000)
  val tableCount = 100
  val tuplesPerTable = 10000
  var client: () => Client = () =>
    Mysql.client
      .withCredentials("root", null)
      .withDatabase("test")
      .newRichClient("localhost:4000")

  try {
    Await.result(
      for {
        _ <- createTables(1).foreach(_ => logger.info("CREATE DONE!"))
        _ <- insertTuples(8).foreach(_ => logger.info("INSERT DONE!"))
        _ <- randomPointGet(8)

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
    } yield client().session(sess => {
      Future.traverseSequentially(chunk) { tableID =>
        for {
          _ <- sess.query(s"DROP TABLE IF EXISTS t_$tableID;").monBy(monitor)
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
      client().session(sess => {
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
    }

    Future.join(futures)
  }

  def randomPointGet(parallel: Int): Future[Unit] = {
    Future.join(Seq.fill(parallel) {
      client().session(sess => {
        Future.whileDo(true) {
          sess
            .query(
              s"SELECT * FROM t_${between(1, tableCount + 1)} WHERE a = ${between(1, tuplesPerTable + 1)};"
            )
            .monBy(monitor)
        }
      })
    })
  }
}
