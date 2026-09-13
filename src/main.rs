use std::time::Duration ;
use tokio::sync::mpsc ;
use tokio::time::{interval, sleep} ;
use tokio_util::sync::CancellationToken ;

async fn worker(
            mut rs: mpsc::Receiver<u32>,
            tr: mpsc::Sender<u32>,
            token: CancellationToken,
        ) {

    println!("[worker] запущен") ;

    loop {
        // Все асинхронные вызовы не содержат .await внутри tokio::select!
        let item = tokio::select! {
            // Чтение приёмного канала
            res = rs.recv() => match res {
                Some(v) => v,
                None => {
                    println!("[worker] tr канал закрыт, вызод.") ;
                    break ;
                },
            },            
            // Future завершится немедленно, если на момент вызова этого метода токен 
            // уже отменен.
            _ = token.cancelled() => {
                println!("[worker] получил сиигнал отмены, выход") ;
                break ;
            },
        } ;

        println!("[worker] Обработка прочитанного значения: {item} из rs") ;

        // имитация длительной обработки
        sleep(Duration::from_millis(300)).await ;

        if tr.send(item * 10).await.is_err() {
            println!("[worker] приёмник результата закрыт, вызод.") ;
            break;
        }

        println!("[worker] результат ртправлен.") ;

    }

    println!("[worker] завершён.") ;
}

// принтер печатающий результаты
async fn printer(mut rs: mpsc::Receiver<u32>) {

    while let Some(v) = rs.recv().await {
        println!("[printer] result: {v}") ;
    }

    println!("[printer] завершён.") ;
}

#[tokio::main]
async fn main() {
    // Создание каналов
    let (work_tr, work_rc) = mpsc::channel::<u32>(16) ;
    let (res_tr, res_rc) = mpsc::channel::<u32>(16) ;

    // Токен отмены
    // Создаёт новый CancellationToken в не отменённом статусе
    let token = CancellationToken::new() ;

    let worker_token = token.clone() ;

    // порождает tokio задачё на основе worker()
    let worker_handl = tokio::spawn(
            worker(
                work_rc,
                res_tr,
                worker_token,
            )
        ) ;

    // порождает tokio задачу на основе printer()
    let printer_handl = tokio::spawn(
        printer(res_rc)
    ) ;

    let producer_token = token.clone() ;
    let producer = tokio::spawn(async move {
        // создает асинхронный таймер-интервал (периодический таймер) из tokio, который 
        // будет срабатывать строго каждые 300 миллисекунд
        let mut tick = interval(Duration::from_millis(300)) ;
        let mut n = 0u32 ;  // начальная установка для отправки

        loop {
            tokio::select! {
                _ = producer_token.cancelled() => {
                    println!("[producer] отмена, выход.") ;
                    break ;
                },
                _ = tick.tick() => {
                    n += 1 ;
                    if work_tr.send(n).await.is_err() {
                        println!("[producer] work_rs закрыть, выход") ;
                        break ;
                    }

                    println!("[producer] задание отправлено.") ;
                }
            }
        }

        drop(work_tr);
    }) ;

    let cancel_token = token.clone() ;

    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await ;
        println!("[main] отсылка сигнала отмены.") ;
        // Отмените CancellationToken и все дочерние токены, созданные на его основе.
        // Это приведет к возобновлению выполнения всех задач, ожидающих отмены.
        cancel_token.cancel();
    }) ;

    let _ = producer.await ;
    let _ = worker_handl.await ;
    let _ = printer_handl.await ;

    println!("[main] корректгная отмена.") ;

}
