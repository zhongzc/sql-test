mod monitor;

use crate::monitor::Monitor;

use std::iter;

use mysql::prelude::*;
use mysql::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;

fn main() {
    let url = "mysql://root@127.0.0.1:4000/test";

    let pool = Pool::new(url).unwrap();
    let monitor = Monitor::start_monitoring(1000);

    println!("CREATING TABLES...");
    create_tables(1, 1, &pool, &monitor);

    // println!("INSERTING TUPLES...");
    // insert_tuples(100, 10, 1000, &pool, &monitor);
    //

    insert_in_batch(1, &pool, &monitor);

    println!("QUERYING...");
    random_query(1, 10, &pool, &monitor);
}

fn insert_in_multi_statments(table_count: usize, pool: &Pool, monitor: &Monitor) {
    (0..10)
        .map(|_| {
            let mut conn = pool.get_conn().unwrap();
            let monitor = monitor.clone();
            std::thread::spawn(move || {
                let mut rand = rand::thread_rng();
                loop {
                    let _g = monitor.exec_once();

                    let table_id = rand.gen_range(0, table_count);
                    let mut sql = String::new();
                    sql.push_str("BEGIN /*T! OPTIMISTIC */;");
                    (0..100)
                        .map(|_| {
                            let num = rand.gen::<i32>();
                            let text_len = rand.gen_range(5, 20);
                            let text = iter::repeat(())
                                .map(|()| rand.sample(Alphanumeric))
                                .take(text_len)
                                .collect::<String>();
                            (num, text)
                        })
                        .for_each(|(num, text)| {
                            sql.push_str(&format!(
                                "INSERT INTO t_{} (a, b, c) VALUES (NULL, {}, '{}');",
                                table_id, num, text
                            ));
                        });
                    sql.push_str("COMMIT;");

                    conn.query_drop(sql).unwrap();
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}

fn insert_in_batch(table_count: usize, pool: &Pool, monitor: &Monitor) {
    (0..1000)
        .map(|_| {
            let mut conn = pool.get_conn().unwrap();
            let monitor = monitor.clone();
            std::thread::spawn(move || {
                let mut rand = rand::thread_rng();
                {
                    let _g = monitor.exec_once();

                    let mut sql = String::new();
                    sql.push_str(&format!(
                        "INSERT INTO t_0 (a, b, c) VALUES ",
                    ));
                    (0..199)
                        .map(|_| {
                            let num = rand.gen::<i32>();
                            let text_len = rand.gen_range(5, 20);
                            let text = iter::repeat(())
                                .map(|()| rand.sample(Alphanumeric))
                                .take(text_len)
                                .collect::<String>();
                            (num, text)
                        })
                        .for_each(|(num, text)| {
                            sql.push_str(&format!("(NULL, {}, '{}'),", num, text));
                        });
                    let num = rand.gen::<i32>();
                    let text_len = rand.gen_range(5, 20);
                    let text = iter::repeat(())
                        .map(|()| rand.sample(Alphanumeric))
                        .take(text_len)
                        .collect::<String>();
                    sql.push_str(&format!("(NULL, {}, '{}');", num, text));

                    conn.query_drop(sql).unwrap();
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}

fn create_tables(count: usize, parallel: usize, pool: &Pool, monitor: &Monitor) {
    let table_ids = (0..count).collect::<Vec<_>>();
    table_ids.chunks(count / parallel).map(|chunk| {
        let mut conn = pool.get_conn().unwrap();
        let monitor = monitor.clone();
        let chunk = Vec::from(chunk);
        std::thread::spawn(move || {
            for i in chunk {
                let _g = monitor.exec_once();
                conn.query_drop(format!("CREATE TABLE IF NOT EXISTS t_{} (a int auto_increment, b int, c varchar(100), primary key (a));", i)).unwrap();
                println!("Table t_{} has been created", i);
            }
        })
    }).collect::<Vec<_>>().into_iter().for_each(|thread| thread.join().unwrap());
}

fn insert_tuples(
    table_count: usize,
    parallel: usize,
    tuple_count_per_table: usize,
    pool: &Pool,
    monitor: &Monitor,
) {
    let table_tuple = (0..table_count)
        .flat_map(|table_id| (0..tuple_count_per_table).map(move |tuple_id| (table_id, tuple_id)))
        .collect::<Vec<_>>();
    table_tuple
        .chunks(table_tuple.len() / parallel)
        .map(|chunk| {
            let monitor = monitor.clone();
            let chunk = Vec::from(chunk);
            let pool = pool.clone();
            std::thread::spawn(move || {
                let mut rand = rand::thread_rng();

                // Insert 100 tuples in a batch
                chunk.chunks(100).for_each(|chunk| {
                    let mut txn = pool.start_transaction(Default::default()).unwrap();
                    chunk.iter().for_each(|(table_id, _)| {
                        let _g = monitor.exec_once();

                        let text_len = rand.gen_range(5, 20);
                        txn.query_drop(&format!(
                            "INSERT INTO t_{} (b, c) VALUES ({}, '{}')",
                            table_id,
                            rand.gen::<i32>(),
                            iter::repeat(())
                                .map(|()| rand.sample(Alphanumeric))
                                .take(text_len)
                                .collect::<String>()
                        ))
                        .unwrap();
                    });

                    let _g = monitor.exec_once();
                    txn.commit().unwrap();
                });
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}

fn random_query(
    table_count: usize,
    parallel: usize,
    pool: &Pool,
    monitor: &Monitor,
) {
    (0..parallel)
        .map(|_| {
            let monitor = monitor.clone();
            let mut conn = pool.get_conn().unwrap();
            std::thread::spawn(move || {
                let mut rand = rand::thread_rng();
                loop {
                    let _g = monitor.exec_once();
                    conn.query_drop(format!(
                        "SELECT count(*) FROM t_0",
                    ))
                    .unwrap();
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}
