use crate::Cache;

use event_stats::{Waits,Event};

use std::hash::Hash;
use std::cmp::Eq;
use std::fmt::Debug;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::time::Instant;
use tokio::sync::Mutex;

pub enum LruAction {
    Attach,
    MoveToHead,
}

// lru is used only to drive move_entry eviction.
// the move_entry cache is separate
#[derive(Clone)]
struct Entry<K: Hash + Eq + Debug>{
    pub key: K,
    //
    pub next: Option<K>,  
    pub prev: Option<K>,
}

impl<K: Hash + Eq + Debug> Entry<K> {
    fn new(k : K) -> Entry<K> {
        Entry{key: k
            ,next: None
            ,prev: None
        }
    }
}


// impl Drop for Entry {
//         fn drop(&mut self) {
//         //println!("\nDROP LRU Entry {:?}\n",self.key);
//     }
// }



struct LRU<K: Hash + Eq + Debug,V> {
    capacity: usize,
    cnt : usize,
    // pointer to Entry value in the LRU linked list for a K
    lookup : HashMap<K,Entry<K>>,
    //
    persist_submit_ch: tokio::sync::mpsc::Sender<(usize, K, Arc<Mutex<V>>, Instant)>,
    // record stat waits
    waits : Waits,
    //
    head: Option<K>,
    tail: Option<K>,
}
    // 371 |       let lru_server = tokio::task::spawn( async move { 
    //     |  __________________________________________^
    // 372 | |         loop {
    // 373 | |             tokio::select! {
    // 374 | |                 //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
    // ...   |
    // 438 | |         }               
    // 439 | |     }); 
    //     | |_____^ future created by async block is not `Send`
    // head: Option<Rc<RefCell<Entry>>>,
    // tail: Option<Rc<RefCell<Entry>>>,


// implement attach & move_to_head as trait methods.
// // Makes more sense however for these methods to be part of the LRU itself - just epxeriementing with traits.
// pub trait LRU {
//     fn attach(
//         &mut self, // , cache_guard:  &mut std::sync::MutexGuard<'_, Cache::<K,V>>
//         K: K,
//     );

//     //pub async fn detach(&mut self
//     fn move_to_head(
//         &mut self,
//         K: K,
//     );
// }


impl<K: Hash + Eq + Debug,V> LRU<K,V> 
where K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send
{  
    pub fn new(
        cap: usize,
        persist_submit_ch: tokio::sync::mpsc::Sender<(usize, K, Arc<tokio::sync::Mutex<V>>, Instant)>,
        waits : Waits
    ) -> Self {
        LRU{
            capacity: cap,
            cnt: 0,
            lookup: HashMap::new(),
            // send channels for persist requests
            persist_submit_ch: persist_submit_ch,
            // record stat waits
            waits,
            //
            head: None,
            tail: None,
        }
    }
    
    
    // async fn print(&self,context : &str) {

    //     let mut entry = self.head.clone();
    //     println!("*** print LRU chain *** len - {} ",context);
    //     let mut i = 0;
    //     while let Some(entry_) = entry {
    //         i+=1;
    //         let k = entry_.lock().await.key.clone();
    //         println!("LRU Print [{}] {} {:?}",context, i, k);
    //         entry = entry_.lock().await.next.clone();      
    //     }
    //     println!("*** print LRU chain DONE ***");
    // }

}
impl<K,V> LRU<K,V> 
where K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send, V:  Clone + Debug
{    
    // prerequisite - move_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRU if lock has been acquired via Arc<Mutex<LRU>>
    async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, Cache::<K,V>>
        task : usize,
        key : K,
        evict_tries: usize,
        cache: Cache<K,V>,
    ) {
        // calling routine (K) is hold lock on V(K)
        // self.print("attach ").await;
        ////println!("   ");
        println!("{} LRU attach {:?}. ***********", task, key);  
        let start_time = Instant::now();

        if self.cnt >= self.capacity {

            let mut lc = 0;  
            let tail = self.tail.as_ref().unwrap().clone();
            //let  tail_entry = self.lookup.get_mut(tail).unwrap();
            //let mut move_entry = tail_entry;
            let mut try_to_evict = tail;


            while self.cnt >= self.capacity && lc < evict_tries  {
                let before =Instant::now();         
                {
                    lc += 1;
                    // ================================
                    // Evict the tail entry in the LRU 
                    // ================================
                    println!("{} LRU: attach reached LRU capacity - evict tail  lru.cnt {}  lc {}  key {:?}", task, self.cnt, lc,  key);
                    // unlink tail move_entry from lru and notify evict service.
                    // Clone REntry as about to purge it from cache.
                    let evict_entry = self.lookup.get(&try_to_evict).unwrap().clone();
                    println!("{} LRU: attach evict processing: try to evict key {:?} lc {}",task, evict_entry.key,lc);
                    // ================================
                    // Lock cache
                    // ================================
                    //let before = Instant::now();
                    let mut cache_guard = cache.0.lock().await;
                    let Some(arc_evict_node_) = cache_guard.data.get(&evict_entry.key)  
                                else { println!("{} LRU: PANIC - attach evict processing: expect entry in cache {:?}",task, evict_entry.key);
                                    panic!("LRU: attach evict processing: expect entry in cache {:?} len {} ",evict_entry.key, cache_guard.data.len());
                                    };
                    let arc_evict_node=arc_evict_node_.clone();
                    // ==========================
                    // acquire lock on evict node 
                    // ==========================
                    let tlock_result = arc_evict_node.try_lock();
                    match tlock_result {

                        Ok(evict_node_guard)  => {
                            // ============================
                            // check state of entry in cache
                            // ============================
                            if cache_guard.inuse(&evict_entry.key, task) {
                                println!("{} LRU attach evict -  cannot evict node as inuse set - abort eviction {:?} after tries {}", task, evict_entry.key, lc );
                                //sleep(Duration::from_millis(10)).await;
                                drop(evict_node_guard);
                                continue;
                            }
                            //println!("{} LRU attach evict -  not in use evict node {:?} tries {}",task,evict_entry.key,lc);
                            cache_guard.set_persisting(evict_entry.key.clone());
                            // ============================
                            // remove node from cache
                            // ============================
                            cache_guard.data.remove(&evict_entry.key);   
                            // ==================
                            // detach evict entry 
                            // ==================
                            if lc == 1 {
                                // from tail
                                match evict_entry.prev {
                                    None => {panic!("LRU attach - evict_entry - expected prev got None")}
                                    Some(ref new_tail) => {
                                        
                                        self.lookup.get_mut(new_tail).unwrap().next = None;
                                        self.tail = Some(new_tail.clone());
                                    }
                                }
                            } else {
                                // remove entry further up from tail
                                //println!("{} LRU attach evict -  evict from further up tail... lc {}",task, lc); 
                                if  evict_entry.next == None {
                                    panic!("LRU attach - evict_entry - expected next Some got None");
                                }
                                let next = evict_entry.next;
                                match evict_entry.prev {
                                    None => {panic!("LRU attach - evict_entry - expected prev got None")}
                                    Some(ref prev) => {
                                        self.lookup.get_mut(prev).unwrap().next = next.clone();
                                        self.lookup.get_mut(&next.unwrap()).unwrap().prev = evict_entry.prev.clone();
                                    }                          
                                }
                            }
                            self.cnt-=1;
                            // =====================
                            // remove from lru lookup 
                            // =====================
                            self.lookup.remove(&evict_entry.key);
                            // ================================
                            // release cache lock
                            // ================================
                            drop(cache_guard);  
                            drop(evict_node_guard); // required by persist 
                            // ===============================================================================
                            // notify persist service - need to set persistence before proceeding 
                            // ================================================================================
                            println!("{} LRU: attach evict - notify persist service to persist {:?}",task, evict_entry.key);
                            if let Err(err) = self
                                .persist_submit_ch
                                .send((task, evict_entry.key.clone(), arc_evict_node.clone(), Instant::now()))
                                .await
                            {
                                println!("{} LRU Error sending on Evict channel: [{}]", task,err);
                            }
                            // =====================================================================
                            // cache lock released - now that submit persist has been sent (queued)
                            // =====================================================================
                        }
                        Err(_err) =>  {
                            // Abort eviction - as node is being accessed.
                            // TODO check if error is "node locked"
                            println!("{} LRU attach - lock cannot be acquired - abort eviction for {:?} lc {}",task, evict_entry.clone().key,lc);
                            drop(cache_guard);  
                            try_to_evict = evict_entry.prev.as_ref().unwrap().clone();
                            continue;
                        }
                    }
                }
                // successfully removed tail entry reset lc and pointers
                lc = 0; 

                self.waits.record(Event::LRUevicting, Instant::now().duration_since(before)).await;  

            }
        }      
        // ======================
        // attach to head of LRU
        // ======================
        {
        let mut attach_entry = Entry::new(key.clone());
        match self.head {
            None => { 
                // empty LRU   
                self.head = Some(key.clone());
                self.tail = Some(key.clone());
                }
            
            Some(ref hd_key) => {
                // set old head prev to point to new entry;
                let head_entry = self.lookup.get_mut(hd_key).unwrap();
                head_entry.prev=Some(key.clone());
                // set new entry next to point to old head entry & prev to NOne   
                attach_entry.next = Some(hd_key.clone());
                // set LRU head to point to new entry
                self.head=Some(key.clone());

                if let None = attach_entry.next {
                    panic!("LRU INCONSISTENCY attach: expected Some for next but got NONE {:?}",key);
                }
                if let Some(_) = attach_entry.prev {
                    panic!("LRU INCONSISTENCY attach: expected None for prev but got NONE {:?}",key);
                }
                if let None = head_entry.prev {
                    panic!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",key);
                }
            }
        }
        if let Some(_) = self.lookup.get(&key) {
            println!("{} LRU INCONSISTENCY - attach entry exists in lookup {:?}",task, key);
            panic!("LRU INCONSISTENCY - attach entry exists in lookup {:?}",key)
        }
        self.lookup.insert(key, attach_entry);
        
        if let None = self.head {
                panic!("LRU INCONSISTENCY attach: expected LRU to have head but got NONE")
        }
        if let None = self.tail {
                panic!("LRU INCONSISTENCY attach: expected LRU to have tail but got NONE")
        }
        self.cnt+=1;
        println!("{} LRU: attach add cnt {}",task, self.cnt);
        }

        self.waits.record(event_stats::Event::LRUAttach,Instant::now().duration_since(start_time)).await; 
 
        //self.print("attach").await;
    }
    
    
    // prerequisite - move_entry has been confirmed to be in lru-cache.\/
    // to execute a method a lock has been taken out on the LRU
    async fn move_to_head(
        &mut self,
        task : usize,
        key: K,
    ) {  
        let start_time = Instant::now();
        {
        
            //println!("--------------");
            println!("{} LRU move_to_head {:?} ********",task, key);
            // abort if move_entry is at head of lru
            match self.head {
                None => {
                    panic!("{} LRU move_to_head expected head to be Some got None",task);
                }
                Some(ref hd_key) => {
                    if *hd_key == key {
                        // k already at head
                        println!("{} LRU move_to_head {:?} at head already, return", task, key);
                        return
                    }    
                }
            }
            // lookup entry in map
            let move_entry = match self.lookup.get(&key) {
                None => {  
                        println!("{} move-to-head PANIC LRU INCONSISTENCY no lookup entry durin  - Reason: task maybe running out of order. {:?}",task, key);
                        panic!("{} move-to-head LRU INCONSISTENCY no lookup entry {:?}",task, key);
                        }
                Some(v) => v.clone()
            };
            {
            // NEW CODE to fix eviction and new request at same time on a Node
            if let None = move_entry.prev {
                    println!("LRU move_to_head INCONSISTENCY move_to_head: expected entry to have prev but got NONE {:?}",key);
                    if let None = move_entry.next {
                        panic!("{} LRU move_to_head : got a entry with no prev or next set (ie. a new node) - some synchronisation gone astray",task)
                    }
            }
            // check if moving tail entry
            if let None = move_entry.next {
                
                    println!("{} LRU move_to_head detach tail entry {:?}",task, key);
                    let prev = move_entry.prev.as_ref().unwrap();
                    //let prev_entry= self.lookup.get(prev).unwrap();
                    self.tail = Some(prev.clone());
                    
                } else {
                    
                    // unplug move_entry from list 
                    let  prev = move_entry.prev.as_ref().unwrap();
                    let mut prev_entry = self.lookup.get_mut(prev).unwrap().clone();
                    prev_entry.next = move_entry.next.clone();
                    
                    let next = move_entry.next.as_ref().unwrap();
                    let next_entry = self.lookup.get_mut(next).unwrap();
                    next_entry.prev = move_entry.prev.clone();
        
                }
                println!("{} LRU move_to_head Detach complete...{:?}",task, key);
            }
            // ATTACH          
            let hd_key: &K = match self.lookup.get(self.head.as_ref().unwrap()) {
                None => {
                    panic!("LRU empty - expected entries");
                }
                Some(hd) => {
                    &hd.key.clone()
                }
            };
            let move_entry = self.lookup.get_mut(&key).unwrap();
            move_entry.next = Some(hd_key.clone());

            let head_entry = self.lookup.get_mut(hd_key).unwrap();
            head_entry.prev = Some(key.clone());
            self.head = Some(key);
        }
        //self.print("move2head").await;
        println!("{} LRU move_to_head complete.",task);

        self.waits.record(event_stats::Event::LRUMoveToHead,Instant::now().duration_since(start_time)).await; 
 
    }
}


pub(crate) fn start_service<K: Eq + Hash + Debug + Clone + Send + Sync + 'static, V: Send + Sync + Clone + Debug + 'static>
(        lru_capacity : usize
        ,cache: Cache<K,V>
        //
        ,mut lru_rx : tokio::sync::mpsc::Receiver<(usize, K, Instant,tokio::sync::mpsc::Sender<bool>, LruAction)>
        ,mut lru_flush_rx : tokio::sync::mpsc::Receiver<tokio::sync::mpsc::Sender<()>>
        //
        ,persist_submit_ch : tokio::sync::mpsc::Sender<(usize, K, Arc<Mutex<V>>, Instant)>
        //
        ,evict_tries : usize
        ,waits : Waits
) -> tokio::task::JoinHandle<()>  {
    // also consider tokio::task::spawn_blocking() which will create a OS thread and allocate task to it.
    // 
    //let (lru_client_ch, lru_client_rx) = tokio::sync::mpsc::channel::<bool>(1);
   
    let mut lru = LRU::new(lru_capacity, persist_submit_ch.clone(), waits.clone());
    
    let lru_server = tokio::task::spawn( async move { 
        println!("LRU service started....");
        loop {
            tokio::select! {
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                Some((task, key, sent_time, client_ch, action)) = lru_rx.recv() => {

                        waits.record(event_stats::Event::ChanLRURcv,Instant::now().duration_since(sent_time)).await;
 
                        match action {
                            LruAction::Attach => {
                                //println!("{} LRU delay {:?} LRU: action Attach {:?}",task, Instant::now().duration_since(sent_time).as_nanos(), key);
                                lru.attach( task, key, evict_tries, cache.clone()).await;

                            }
                            LruAction::MoveToHead => {
                                //println!("{} LRU delay {:?} LRU: action move_to_head {:?}",task, Instant::now().duration_since(sent_time).as_nanos(),  key);
                                lru.move_to_head( task, key).await;
                            }
                        }
                        // send response back to client...sync'd.
                        if let Err(err) = client_ch.send(true).await {
                            panic!("LRU action send to client_ch {} ",err);
                        };

                    }

                Some(client_ch) = lru_flush_rx.recv() => {
                               
                            println!("LRU service - flush lru ");
                                    let mut entry = lru.lookup.get(&lru.head.clone().unwrap());
                                    let cache_guard = cache.0.lock().await;
                                    println!("LRU flush in progress..persist entries in LRU flust {} lru entries",lru.cnt);
                                    while let Some(entry_) = entry {
                     
                                            let k = entry_.key.clone();
                                            if let Some(arc_node) = cache_guard.data.get(&k) {
                                                if let Err(err) = lru.persist_submit_ch // persist_flush_ch
                                                            .send((0, k, arc_node.clone(), Instant::now()))
                                                            .await {
                                                                println!("Error on persist_submit_ch channel [{}]",err);
                                                            }
                                            } 

                                            entry = lru.lookup.get(&entry_.next.clone().unwrap());  
                                             
                                    }
                                    //sleep(Duration::from_millis(2000)).await;
                                    println!("LRU: flush-persist Send on client_ch ");
                                    if let Err(err) = client_ch.send(()).await {
                                        panic!("LRU send on client_ch {} ",err);
                                    };
                                    println!("LRU shutdown ");
                                    return (); 
                    }                    
            }    
        } 
        //println!("LRU service shutdown....");          
    }); 

    lru_server
}