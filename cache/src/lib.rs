
pub mod service;
pub extern crate event_stats; // makes crate public,
//extern crate event_stats;        // private crate 

use std::sync::Arc;
use std::cmp::Eq;
use std::hash::Hash;
use std::fmt::Debug;
use std::collections::HashSet;
use std::collections::HashMap;

pub use crate::service::lru;

use tokio::time::{sleep, Duration, Instant};
use tokio::sync::Mutex;
// ===========
// re-export
// ===========
pub use event_stats::{Waits,Event};


const LRU_CAPACITY : usize = 40;
const MAX_SP_TASKS : usize = 18;

pub enum CacheValue<V> {
    New(V),
    Existing(V),
}

#[trait_variant::make(Persistence: Send)]
pub trait Persistence_<K, D> 
{

    async fn persist(
        &mut self
        ,task : usize
        ,db : D
        ,waits : Waits
        ,persist_completed_send_ch : tokio::sync::mpsc::Sender<(K, usize)>
    );
}

// Message sent on Evict Queued Channel
pub struct QueryMsg<K>(pub K, pub tokio::sync::mpsc::Sender<bool>, pub usize);

impl<K> QueryMsg<K>{
    fn new(rkey: K, resp_ch: tokio::sync::mpsc::Sender<bool>, task: usize) -> Self {
        QueryMsg(rkey, resp_ch, task)
    }
}

pub trait NewValue<K: Clone,V> {

    fn new_with_key(key : &K) -> Arc<tokio::sync::Mutex<V>>;
}

// =======================
//  Generic Cache that supports persistence
// =======================
// cache responsibility is to synchronise access to db across multiple Tokio tasks on a single cache entry.
// The state of the node edge will determine the type of update required, either embedded or OvB.
// Each cache update will be saved to db to keep both in sync.
// All mutations of the cache hashmap need to be serialized.
#[derive(Debug)]
pub struct InnerCache<K,V> {
    pub datax : HashMap<K, Arc<tokio::sync::Mutex<V>>>,
    // channels
    persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>,
    lru_ch : tokio::sync::mpsc::Sender<(usize, K, Instant, tokio::sync::mpsc::Sender<bool>, lru::LruAction)>,
    // state of K in cache
    //evicted : HashSet<K>,
    inuse : HashMap<K,u8>,
    persisting: HashSet<K>,
    // performance stats rep
    waits : Waits,
    lru_flush_ch : tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>,
    persist_shutdown_ch : tokio::sync::mpsc::Sender<u8>,
    persist_srv : Option<tokio::task::JoinHandle<()>>,

}

#[derive(Debug)]
pub struct Cache<K,V>(pub Arc<Mutex<InnerCache<K,V>>>);


impl<K,V> Cache<K,V>
where K: Clone + std::fmt::Debug + Eq + std::hash::Hash + std::marker::Sync + Send + 'static, 
      V: Clone + std::fmt::Debug + std::marker::Sync + Send +  'static
{

    pub fn new<D: Clone + std::marker::Sync + Send + 'static >(
        waits : Waits
        ,db : D
    ) -> Self
    where V: Persistence<K,D>
   { 
        let (lru_ch, lru_operation_rx) = tokio::sync::mpsc::channel::<(usize, K, Instant,tokio::sync::mpsc::Sender<bool>, lru::LruAction)>(MAX_SP_TASKS+1);
        let (persist_query_ch, persist_query_rx) = tokio::sync::mpsc::channel::<QueryMsg<K>>(MAX_SP_TASKS * 2); 
        let (lru_flush_ch, lru_flush_rx) = tokio::sync::mpsc::channel::<tokio::sync::mpsc::Sender<()>>(1);
        let (persist_shutdown_ch, persist_shutdown_rx) = tokio::sync::mpsc::channel::<u8>(1);
  
        let cache = Cache::<K,V>(Arc::new(tokio::sync::Mutex::new(InnerCache::<K,V>{
                datax: HashMap::new()
                ,persist_query_ch
                ,lru_ch
                //,evicted : HashSet::new()
                ,inuse : HashMap::new()
                ,persisting: HashSet::new()
                //
                ,waits: waits.clone()
                ,lru_flush_ch
                ,persist_shutdown_ch
                ,persist_srv : None
                })));

        // =========================================
        // 4. create channels and start LRU service 
        // ======================================== 
        let (lru_persist_submit_ch, persist_submit_rx) = tokio::sync::mpsc::channel::<(usize, K, Arc<Mutex<V>>, tokio::sync::mpsc::Sender<bool>)>(MAX_SP_TASKS);

        let _ = service::lru::start_service::<K,V>(
                                        LRU_CAPACITY
                                        , cache.clone()
                                        , lru_operation_rx
                                        , lru_flush_rx
                                        , lru_persist_submit_ch
                                        , waits.clone()); 

        // ================================================
        // 3. start persist service
        // ================================================
        println!("start persist service...");
        // 
        let persist_service: tokio::task::JoinHandle<()> = service::persist::start_service::<K,V,D>(
            cache.clone(),
            db,
            persist_submit_rx,
            persist_query_rx,
            persist_shutdown_rx,
            waits.clone(),
        );

        cache.set_persist_srv(persist_service);

        cache

    }
 
    pub async fn set_persist_srv(
        &self
        ,p : tokio::task::JoinHandle<()> 
    ) {
        let mut inner_guard = self.0.lock().await;
        inner_guard.persist_srv = Some(p)
    }


    pub async fn shutdown(&self) {

        let (client_ch, mut client_rx) = tokio::sync::mpsc::channel::<()>(1); 

        println!("cache: lru flush...");
        let mut guard = self.0.lock().await;
        if let Err(err) = guard.lru_flush_ch.send(client_ch).await {
                panic!("cache: LRU send on client_ch {} ",err);
        };
        println!("cache: waiting lru flush to finish...");
        let _ = client_rx.recv().await;
        println!("cache: sleep.. wait for LRU persists to finish..."); 
        //sleep(Duration::from_millis(5000)).await;

        if let Err(err) = guard.persist_shutdown_ch.send(1).await {
            panic!("cache: LRU send on persist_shutdown_ch {} ",err);
        };

        if let Some(ref mut persist) = guard.persist_srv {
            let _ = persist.await;
        }
    }


}

impl<K : Hash + Eq + Debug + Clone,V : Clone + Debug >  InnerCache<K,V>
{
    pub fn unlock(&mut self, key: &K) {
        //println!("InnerCache unlock [{:?}]",key);
        self.unset_inuse(key);
    }

    pub fn set_inuse(&mut self, key: K) {
        //println!("InnerCache set_inuse [{:?}]",key);
        self.inuse.entry(key.clone()).and_modify(|i|*i+=1).or_insert(1);
    }

    pub fn unset_inuse(&mut self, key: &K) {
        //println!("InnerCache unset_inuse [{:?}]",key);
        self.inuse.entry(key.clone()).and_modify(|i|*i-=1);
    }

    pub fn inuse(&self, key: &K) -> bool {
        //println!("InnerCache inuse [{:?}]",key);
        match self.inuse.get(key) {
            None => false,
            Some(i) => {*i > 0},
        }
    }

    pub fn set_persisting(&mut self, key: K) {
        self.persisting.insert(key);
    }

    pub fn unset_persisting(&mut self, key: &K) {
        self.persisting.remove(key);
    }

    pub fn persisting(&self, key: &K) -> bool {
        match self.persisting.get(key) {
            None => false,
            Some(_) => true,
        }
    }

}

impl<K,V> Clone for Cache<K,V> where K : Hash + Eq + Debug + Clone, V:  Clone + Debug {

    fn clone(&self) -> Self {
        Cache::<K,V>(self.0.clone())
    }
}

impl<K: Hash + Eq + Clone + Debug, V:  Clone + NewValue<K,V> + Debug>  Cache<K,V>
{

    pub async fn unlock(&mut self, key: &K) {
        println!("CACHE: cache.unlock {:?}",key);
        self.0.lock().await.unset_inuse(key);
        println!("CACHE: cache.unlock DONE");
    }

    pub async fn get(
        &self
        ,key : &K
        ,task : usize,
    ) -> CacheValue<Arc<tokio::sync::Mutex<V>>> {
        let (lru_client_ch, mut srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1); 

        let before:Instant;  
        let mut cache_guard = self.0.lock().await;
        match cache_guard.datax.get(&key) {
            
            None => {
                println!("{} CACHE: - Not Cached: add to cache {:?}", task, key);
                let lru_ch = cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persist_query_ch = cache_guard.persist_query_ch.clone();
                                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value = V::new_with_key(key);
                // =========================
                // add to cache, set in-use 
                // =========================
                cache_guard.datax.insert(key.clone(), arc_value.clone()); // self.clone(), arc_value.clone());
                cache_guard.set_inuse(key.clone());
                let persisting = cache_guard.persisting(&key);
                // ===============================================================
                // serialise access to value - prevents concurrent operations on key
                // ===============================================================                
                let value_guard = arc_value.lock().await;
                // ============================================================================================================
                // release cache lock with value still locked - value now in cache, so next get on key will go to in-cache path
                // ============================================================================================================
                drop(cache_guard);
                // =======================
                // IS NODE BEING PERSISTED 
                // =======================
                if persisting {
                    println!("{} CACHE: - Not Cached: waiting on persisting due to eviction {:?}",task, key);
                    self.wait_for_persist_to_complete(task, key.clone(),persist_query_ch, waits.clone()).await;
                }
                before =Instant::now();
                if let Err(err) = lru_ch.send((task, key.clone(), before, lru_client_ch, lru::LruAction::Attach)).await {
                    panic!("Send on lru_attach_ch errored: {}", err);
                }   
                waits.record(Event::LRUSendAttach,Instant::now().duration_since(before)).await;    
                // sync'd: wait for LRU operation to complete - just like using a mutex is synchronous with operation.
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::Attach,Instant::now().duration_since(before)).await; 

                return CacheValue::New(arc_value.clone());
            }
            
            Some(arc_value) => {

                //println!("key add_reverse_edge: - Cached key {:?}", task, self);
                // acquire lock on value and release cache lock - this prevents concurrent updates to value 
                // and optimises cache concurrency by releasing lock asap
                let arc_value=arc_value.clone();

                let persist_query_ch = cache_guard.persist_query_ch.clone();
                let lru_ch=cache_guard.lru_ch.clone();
                let waits = cache_guard.waits.clone();
                let persisting = cache_guard.persisting(&key);
                cache_guard.set_inuse(key.clone()); // prevents concurrent persist
                // =========================
                // release cache lock
                // =========================
                drop(cache_guard);
                // =============================================
                // serialise processing on concurrent key-value
                // =============================================
                let value_guard = arc_value.lock().await;
                // ======================
                // IS NODE persisting 
                // ======================
                if persisting {
                    println!("{} CACHE key: in CACHE check if still persisting ....{:?}", task,key);
                    self.wait_for_persist_to_complete(task, key.clone(),persist_query_ch, waits.clone()).await;       
                } 

                before =Instant::now();    
                if let Err(err) = lru_ch.send((task, key.clone(), before, lru_client_ch, lru::LruAction::Move_to_head)).await {
                    panic!("Send on lru_move_to_head_ch failed {}",err)
                };
                waits.record(Event::LRUSendMove,Instant::now().duration_since(before)).await; 
                let _ = srv_resp_rx.recv().await;
                waits.record(Event::MoveToHead,Instant::now().duration_since(before)).await;

                return CacheValue::Existing(arc_value.clone());
            }
        }
    }

    async fn wait_for_persist_to_complete(
        &self
        ,task: usize
        ,key: K  
        ,persist_query_ch : tokio::sync::mpsc::Sender<QueryMsg<K>>
        ,waits : Waits
    )  {
        let (persist_client_send_ch, mut persist_srv_resp_rx) = tokio::sync::mpsc::channel::<bool>(1);
        // wait for evict service to give go ahead...(completed persisting)
        // or ack that it completed already.
        let mut before:Instant =Instant::now();
        if let Err(e) = persist_query_ch
                                .send(QueryMsg::new(key.clone(), persist_client_send_ch.clone(),task))
                                .await
                                {
                                    panic!("evict channel comm failed = {}", e);
                                }
        waits.record(Event::ChanPersistQuery,Instant::now().duration_since(before)).await;

        // wait for persist to complete
        before =Instant::now();
        let persist_resp = match persist_srv_resp_rx.recv().await {
                    Some(resp) => resp,
                    None => {
                        panic!("communication with evict service failed")
                        
                    }
                    };
        waits.record(Event::ChanPersistQueryResp,Instant::now().duration_since(before)).await;
            
        if persist_resp {
                    // ====================================
                    // wait for completed msg from Persist
                    // ====================================
                    println!("{} CACHE: wait_for_persist_to_complete entered...wait for io to complete...{:?}",task, key);
                    before =Instant::now();
                    persist_srv_resp_rx.recv().await;
                    waits.record(Event::ChanPersistWait,Instant::now().duration_since(before)).await;
                }
    }
}




