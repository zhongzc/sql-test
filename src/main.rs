mod monitor;

use crate::monitor::Monitor;

use std::iter;
use std::time::Instant;

use mysql::prelude::*;
use mysql::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;

fn main() {
    let url = "mysql://root@localhost:4000/test";

    let pool = Pool::new(url).unwrap();
    let monitor = Monitor::start_monitoring(1000);

    println!("CREATING TABLES...");
    create_tables(10000, 10, &pool, &monitor);

    println!("INSERTING TUPLES...");
    insert_tuples(10000, 10, 1000, &pool, &monitor);

    println!("QUERYING...");
    random_query(10000, 10, 1000, &pool, &monitor);
}

fn create_tables(count: usize, parallel: usize, pool: &Pool, monitor: &Monitor) {
    let table_ids = (0..count).collect::<Vec<_>>();
    table_ids.chunks(count / parallel).map(|chunk| {
        let mut conn = pool.get_conn().unwrap();
        let monitor = monitor.clone();
        let chunk = Vec::from(chunk);
        std::thread::spawn(move || {
            for i in chunk {
                monitor.incr_count(1);
                let timer = Instant::now();
                conn.exec_drop(format!("CREATE TABLE IF NOT EXISTS t_{} (a int auto_increment, b int, c varchar(100), primary key (a));", i), ()).unwrap();
                monitor.add_response_time(timer.elapsed());
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
                chunk.chunks(100).for_each(|chunk| {
                    let mut txn = pool.start_transaction(Default::default()).unwrap();
                    chunk.iter().for_each(|(table_id, _)| {
                        let timer = Instant::now();
                        monitor.incr_count(1);

                        let str_count = rand.gen_range(5, 20);
                        txn.exec_drop(
                            &format!(
                                "INSERT INTO t_{} (b, c) VALUES ({}, '{}')",
                                table_id,
                                rand.gen::<i32>(),
                                iter::repeat(())
                                    .map(|()| rand.sample(Alphanumeric))
                                    .take(str_count)
                                    .collect::<String>()
                            ),
                            (),
                        )
                        .unwrap();

                        monitor.add_response_time(timer.elapsed());
                    });

                    let timer = Instant::now();
                    monitor.incr_count(1);
                    txn.commit().unwrap();
                    monitor.add_response_time(timer.elapsed());
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
    tuple_count_per_table: usize,
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
                    let timer = Instant::now();
                    monitor.incr_count(1);
                    conn.query_drop(format!(
                        "SELECT * FROM t_{} WHERE a = {}",
                        rand.gen_range(0, table_count),
                        rand.gen_range(1, tuple_count_per_table + 1)
                    ))
                    .unwrap();
                    monitor.add_response_time(timer.elapsed());
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}
