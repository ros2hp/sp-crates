use crate::Cache;
use crate::Persistence;

use event_stats::Waits;

use std::collections::{HashMap,  VecDeque};
use std::sync::Arc;

use tokio::task;
use tokio::sync::Mutex;
use tokio::time::Instant;

//const MAX_PRESIST_TASKS: u8 = 16;

struct Lookup<K,V>(HashMap<K, Arc<Mutex<V>>>);


impl<K,V> Lookup<K,V> {
    fn new() -> Lookup<K,V> {
        Lookup::<K,V>(HashMap::new())
    }
}

// Pending persistion queue
struct PendingQ<K>(VecDeque<K>);


impl<K: std::cmp::PartialEq> PendingQ<K> {
    fn new() -> Self {
        PendingQ::<K>(VecDeque::new())
    }
}

//  container for clients querying persist service
struct QueryClient<K>(HashMap<K, VecDeque<tokio::sync::mpsc::Sender<bool>>>);

impl<K> QueryClient<K> {
    fn new() -> Self {
        QueryClient::<K>(HashMap::new())
    }
}

// struct Persisted(HashSet<K>);


// impl Persisted {

//     fn new() -> Arc<Mutex<Persisted>> {
//        Arc::new(Mutex::new(Persisted(HashSet::new())))
//     }
// }

pub(crate) fn start_service<K,V,D>(
    cache: Cache<K,V>,
    db : D,
    // channels
    mut submit_rx: tokio::sync::mpsc::Receiver<(usize, K, Arc<Mutex<V>>, tokio::time::Instant)>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<u8>,
    //
    waits_ : Waits,
    persist_tasks : usize
) -> task::JoinHandle<()> 
where K: Clone + std::fmt::Debug + Eq + std::hash::Hash + Send + Sync + 'static, 
      V: Clone + Persistence<K,D> + std::fmt::Debug + 'static,
      D: Clone + Send + Sync + 'static
{

    println!("PERSIST  starting persist service: table ");

    //let mut persisted = Persisted::new(); // temmporary - initialise to zero ovb metadata when first persisted
    let mut persisting_lookup: Lookup<K, V> = Lookup::new();
    let mut pending_q: PendingQ<K> = PendingQ::new();
    let mut tasks = 0;

    // persist completed channel - persisting task sends message to Persist Service when it completes 
    let (persist_completed_send_ch, mut persist_completed_rx) =
        tokio::sync::mpsc::channel::<(K,usize,tokio::time::Instant)>(persist_tasks);

    let waits = waits_.clone();

    // start Tokio Task that is the Persist service
    let persist_server = tokio::spawn(async move {

        loop {
            //let persist_complete_send_ch_=persist_completed_send_ch.clone();
            tokio::select! { 
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_rx channel.
                Some((task, key, arc_node, sent_time)) = submit_rx.recv() => {

                        waits.record(event_stats::Event::ChanPersistSubmitRcv,Instant::now().duration_since(sent_time)).await;
 
                        // check if already submitted - 
                        if persisting_lookup.0.contains_key(&key) {
                            println!("{} PANIC: Persist service: submitted key again. Timing Issue: persist yet to finish from previous submit {:?}",task, key);
                            panic!("{} Persist service: submitted key again, persist yet to finish from previous submit {:?}",task, key);
                        };
 
                        // persisting_lookup arc_node for given K
                        println!("{} PERSIST: submit persist for {:?} tasks [{}]",task, key, tasks);
                        persisting_lookup.0.insert(key.clone(), arc_node.clone());

                        if tasks >= persist_tasks {
                            // maintain a FIFO of evicted nodes
                            println!("{} PERSIST: submit - max tasks reached add {:?} pending_q {}",task , key, pending_q.0.len());
                            pending_q.0.push_front(key.clone());                         
                        } else {
		                    // =================================================
                            // lock arc value to access type parameter V [RNode]
                            // =================================================
                            let node_guard_=arc_node.lock().await;

                            let mut node_guard=node_guard_.clone();
                            // spawn async task to persist node
                            let persist_completed_send_ch_=persist_completed_send_ch.clone();
                            let waits=waits.clone();
                            let db=db.clone();
                            
                            tasks+=1;
    
                            tokio::spawn( async  move {
                                // save Node data to db
                                node_guard.persist(
                                    task
                                    ,db
                                    ,waits
                                ).await;
                                // send persist completed msg 
                                let before = Instant::now();
                                if let Err(err) = persist_completed_send_ch_.send((key.clone(), task, before)).await {
                                            println!(
                                                    "Sending completed persist msg to waiting client failed: {}",
                                                            err
                                                );
                                }
                            });
                        }
                        println!("{} PERSIST: submit - Exit",task);

                },

                Some((persist_key, task, sent_time)) = persist_completed_rx.recv() => {

                    waits.record(event_stats::Event::ChanPersistCompleteRcv,Instant::now().duration_since(sent_time)).await;
 
                    tasks-=1;

                    println!("{} PERSIST : completed msg:  key {:?} tasks {}, pending_q {}", task, persist_key, tasks,pending_q.0.len());
                    persisting_lookup.0.remove(&persist_key);
                    cache.0.lock().await.unset_persisting(&persist_key);
                    // // process next node in persist Pending Queue
                    if let Some(queued_key) = pending_q.0.pop_back() {
                        //println!("{} PERSIST : persist next entry in pending_q.... {:?}", task, queued_key);
                        // spawn async task to persist node
                        let persist_completed_send_ch_=persist_completed_send_ch.clone();

                        let Some(arc_node_) = persisting_lookup.0.get(&queued_key) else {panic!("Persist service: expected arc_node in Lookup {:?}",queued_key)};
                        let _arc_node=arc_node_.clone();
                        let db=db.clone();
                        let waits=waits.clone();
                        let mut node_guard = arc_node_.lock().await.clone();
                        //let persisted_=persisted.clone();
                        tasks+=1;
       
                        tokio::spawn(async move {
                            // save Node data to db
                            node_guard.persist(
                                task
                                ,db
                                ,waits
                            ).await;
                            // send task completed msg to self
                            let before=Instant::now();
                            if let Err(err) = persist_completed_send_ch_.send((queued_key.clone(), task, before)).await {
                                            println!(
                                                "Sending completed persist msg to waiting client failed: {}",
                                                err
                                            );
                            }
                        });
                    }
                    //println!("{} PERSIST finished completed msg:  key {:?}  tasks {} ", task, persist_key, tasks);
                },

                _ = shutdown_rx.recv() => {
                        println!("PERSIST shutdown:  Waiting for remaining persist tasks [{}] pending_q {} to complete...",tasks as usize, pending_q.0.len());
                        while tasks > 0 || pending_q.0.len() > 0 {
                            println!("  PERSIST : shutdown wait for completed msg:  tasks {}, pending_q {}", tasks,pending_q.0.len());
                            if tasks > 0 {
                                let Some(persist_key) = persist_completed_rx.recv().await else {panic!("Inconsistency; expected task complete msg got None...")};
                                tasks-=1;
                            }
                            if let Some(queued_key) = pending_q.0.pop_back() {
  
                                let persist_completed_send_ch_=persist_completed_send_ch.clone();
                                let Some(arc_node_) = persisting_lookup.0.get(&queued_key) else {panic!("Persist service: expected arc_node in Lookup")};
                                let waits=waits.clone();
                                let mut node_guard= arc_node_.lock().await.clone();
                                let db=db.clone();
                                tasks+=1;

                                //println!("PERSIST: shutdown  persist task tasks {} Pending-Q {}", tasks, pending_q.0.len() );
                                // save Node data to db
                                tokio::spawn(async move {
                                    // save Node data to db
                                    node_guard.persist(
                                        0
                                        ,db
                                        ,waits
                                    ).await;
                                    // send task completed msg to self
                                    let before = Instant::now();
                                    if let Err(err) = persist_completed_send_ch_.send((queued_key.clone(), 0, before)).await {
                                        println!(
                                            "Sending completed persist msg to waiting client failed: {}",
                                            err
                                        );
                                    }
                            });
                            }
                        }
                        println!("PERSIST  shutdown completed. Tasks {}",tasks);
                        return;
                },
            }
        } // end-loop
    });
    persist_server
}


