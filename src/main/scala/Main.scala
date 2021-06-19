import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.util.{Await, Future}

object Main extends App {
  val monitor = Monitor.startMonitoring(5000)

  try {
    val client = Mysql.client
      .withCredentials("root", null)
      .withDatabase("test")
      .newRichClient("localhost:4000")

    Await.result(
      for {
        _ <- createTables(client, monitor, 100, 1)

     // _ <- other steps...
      } yield ()
    )
  } finally {
    monitor.stop()
  }

  def createTables(client: Client, monitor: Monitor, count: Int, parallel: Int): Future[()] = {
    import Monitor.MonitoredFuture

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
                  s"CREATE TABLE t_$table_id (" +
                    "a int AUTO_INCREMENT," +
                    "b int," +
                    "c varchar(100)," +
                    "primary key (a)" +
                  ");"
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
