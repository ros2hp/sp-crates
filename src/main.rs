mod node;
mod rkey;

mod service;
mod types;

extern crate lrucache;
use lrucache::event_stats; //{Waits, Event};

use std::collections::HashMap;
use std::env;
use std::mem;
use std::string::String;
//use std::sync::LazyLock;


use lrucache::Cache;
use node::RNode;

use rkey::RKey;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::builders::PutRequestBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::WriteRequest;
use aws_sdk_dynamodb::Client as DynamoClient;

use uuid::Uuid;

use mysql_async::prelude::*;

use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};
//use tokio::task::spawn;

const DYNAMO_BATCH_SIZE: usize = 25;
//pub const LRU_CAPACITY: usize = 40;

const LS: u8 = 1;
const LN: u8 = 2;
const LB: u8 = 3;
const LBL: u8 = 4;
const _LDT: u8 = 5;

// ==============================================================================
// Overflow block properties - consider making part of a graph type specification
// ==============================================================================

// EMBEDDED_CHILD_NODES - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent RNode data,
// which will be an overhead in circumstances where child data is not required.
const EMBEDDED_CHILD_NODES: usize = 4;//50; //10; // prod value: 20

// MAX_OV_BLOCKS - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
const MAX_OV_BLOCKS: usize = 5; // prod value : 100

// OV_MAX_BATCH_SIZE - number of items to an overflow batch. Always fixed at this value.
// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
// An overflow block has an unlimited number of batches.
const OV_MAX_BATCH_SIZE: usize = 160;//160; //15; // Prod 100 to 500.

// OV_BATCH_THRESHOLD, initial number of batches in an overflow block before creating new Overflow block.
// Once all overflow blocks have been created (MAX_OV_BLOCKS), blocks are randomly chosen and each block
// can have an unlimited number of batches.
const OV_BATCH_THRESHOLD: usize = 4; //100

type SortK = String;
type Cuid = Uuid;
type Puid = Uuid;

// Overflow Block (Uuids) item. Include in each propagate item.
// struct OvB {
//      ovb: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
//      xf: Vec<AttributeValue>, // used in uid-predicate 3 : ovefflow UID, 4 : overflow block full
// }

// struct ReverseEdge {
//     pk: AttributeValue, // cuid
//     sk: AttributeValue, // R#sk-of-parent|x    where x is 0 for embedded and non-zero for batch id in ovb
//     //
//     tuid: AttributeValue, // target-uuid, either parent-uuid for embedded or ovb uuid
//     tsk: String,
//     tbid: i32,
//     tid: i32,
// }
//
// struct OvBatch {
//     pk: Uuid, // ovb Uuid
//     //
//     nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
//     xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block ful
// }

struct ParentEdge {
    //
    //nd: Vec<AttributeValue>, //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
    // xf: Vec<AttributeValue>, // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
    // id: Vec<u32>,            // most recent batch in overflow
    // //
    // ty: String,         // node type m|P
    // p: String,          // edge predicate (long name) e.g. m|actor.performance - indexed in P_N
    // cnt: usize,         // number of edges < 20 (MaxChildEdges)
    // rrobin_alloc: bool, // round robin ovb allocation applies (initially false)
    // eattr_nm: String,   // edge attribute name (derived from sortk)
    // eattr_sn: String,   // edge attribute short name (derived from sortk)
    //                     //
                        // ovb_idx: usize, // last ovb populated
                        // ovbs: Vec<Vec<OvBatch>>, //  each ovb is made up of batches. each ovb simply has a different pk - a batch shares the same pk.
                        //                          //
                        //rvse: Vec<ReverseEdge>,
}

struct PropagateScalar {
    entry: Option<u8>,
    psk: String,
    sk: String,
    // scalars
    ls: Vec<AttributeValue>,
    ln: Vec<AttributeValue>, // merely copying values so keep as Number datatype (no conversion to i64,f64)
    lbl: Vec<AttributeValue>,
    lb: Vec<AttributeValue>,
    ldt: Vec<AttributeValue>,
    // reverse edges
    cuids: Vec<Uuid>, //Vec<AttributeValue>,
}

enum Operation {
    Attach(ParentEdge), // not used
    Propagate(PropagateScalar),
}

// pub struct Dynamo<T> {
//     pub  conn : T,
//     pub table_name : String
// }

#[derive(Clone)]
pub struct Dynamo {
    pub conn: DynamoClient,
    pub table_name: String,
}

impl Dynamo {
    fn new(conn: DynamoClient, table_name: impl ToString) -> Self {
        Dynamo {
            conn,
            table_name: table_name.to_string(),
        }
    }
}

// impl<DynamoClient> DB for Dynamo<DynamoClient> {
//     fn get_conn(&self) -> DynamoClient {
//         self.conn
//     }
//     fn get_table_name(&self) -> String {
//         self.table_name
//     }
// }

// impl DB for Dynamo {

//     type conn_str = DynamoClient;

//     fn get_conn(&self) -> DynamoClient {
//          self.conn.clone()
//     }
//     fn get_table_name(&self) -> String {
//         self.table_name.clone()
//     }
// }

#[::tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
    let mut task: usize = 0;
    // ===============================
    // 1. Source environment variables
    // ===============================
    let mysql_host =
        env::var("MYSQL_HOST").expect("env variable `MYSQL_HOST` should be set in profile");
    let mysql_user =
        env::var("MYSQL_USER").expect("env variable `MYSQL_USER` should be set in profile");
    let mysql_pwd =
        env::var("MYSQL_PWD").expect("env variable `MYSQL_PWD` should be set in profile");
    let mysql_dbname =
        env::var("MYSQL_DBNAME").expect("env variable `MYSQL_DBNAME` should be set in profile");
    let max_sp_tasks_ =
        env::var("MAX_SP_TASKS").expect("env variable `MAX_SP_TASKS` should be set in profile");
    let max_persist_tasks_ =
        env::var("MAX_PERSIST_TASKS").expect("env variable `MAX_PERSIST_TASKS` should be set in profile");
    let graph = env::var("GRAPH_NAME").expect("env variable `GRAPH_NAME` should be set in profile");
    let lru_capacity_ = env::var("LRU_CAPACITY").expect("env variable `LRU_CAPACITY` should be set in profile");
    let table_name = "RustGraph.dev.11";
    let evict_tries = "3";
    // ===========================
    // 2. Print config
    // ===========================
    println!("========== Config ===============  ");
    println!("Config: max_sp_tasks:   {}", max_sp_tasks_);
    println!("Config: max_persist_tasks:   {}", max_persist_tasks_);
    println!("Config: lru_capacity:   {}", lru_capacity_);
    println!("Config: Table name:     {}", table_name);
    println!("Config: DateTime :      {:?}", Instant::now());
    println!("Config: Evict_Tries :   {:?}", evict_tries);
    println!("=================================  ");

    let max_sp_tasks = usize::from_str_radix(&max_sp_tasks_, 10).unwrap();
    let persist_tasks = usize::from_str_radix(&max_persist_tasks_, 10).unwrap();
    let lru_capacity = usize::from_str_radix(&lru_capacity_,10).unwrap();
    // ===========================
    // 2. Create a Dynamodb Client
    // ===========================
    let config = aws_config::from_env().region("us-east-1").load().await;
    let dynamo_client = DynamoClient::new(&config);
    // =======================================
    // 3. Fetch Graph Data Types from Dynamodb
    // =======================================
    let (node_types, graph_prefix_wdot) = types::fetch_graph_types(&dynamo_client, graph).await?;

    for t in node_types.0.iter() {
        println!(
            "RNode type {} [{}]    reference {}",
            t.get_long(),
            t.get_short(),
            t.is_reference()
        );
        // for &attr in t.iter() {
        //     println!("attr.name [{}] dt [{}]  c [{}]", attr.name, attr.dt, attr.c);
        // }
    }
    // create broadcast channel to shutdown services
    let (shutdown_broadcast_sender, _) = broadcast::channel(1); // broadcast::channel::<u8>(1);

    // =====================
    // setup Stats recorder
    // =====================
    let (stats_ch, stats_rx) =
        tokio::sync::mpsc::channel::<(event_stats::Event, Duration, Duration)>(max_sp_tasks * 10);
    let waits = event_stats::Waits::new(stats_ch.clone());
    // =====================
    // shutdown channel
    // =====================
    let stats_shutdown_ch = shutdown_broadcast_sender.subscribe();

    // =============================================
    // start Retry service (handles failed putitems)
    // =============================================
    println!("start Retry service...");
    let (retry_ch, retry_rx) = tokio::sync::mpsc::channel(max_sp_tasks * 2);
    let retry_shutdown_ch = shutdown_broadcast_sender.subscribe();
    let retry_service = service::retry::start_service(
        dynamo_client.clone(),
        retry_rx,
        retry_ch.clone(),
        retry_shutdown_ch,
        table_name,
    );

    let stats_service = event_stats::start_event_service(stats_rx, stats_shutdown_ch);

    // ===========================================
    // 3. allocate queued_key - with database config
    // ===========================================
    let db: Dynamo = Dynamo::new(dynamo_client.clone(), table_name.to_string());
    let Ok(evict_tries_) = evict_tries.parse() else {  panic!("evict_tries cannot be parsed to usize")};
    let reverse_edge_cache = Cache::<RKey, RNode>::new(max_sp_tasks, waits.clone(), evict_tries_, db, lru_capacity, persist_tasks).await;

    // ================================
    // 5. Setup a MySQL connection pool
    // ================================
    let pool_opts = mysql_async::PoolOpts::new()
        .with_constraints(mysql_async::PoolConstraints::new(5, 30).unwrap())
        .with_inactive_connection_ttl(Duration::from_secs(60));

    let mysql_pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::default()
            //.from_url(url)
            .ip_or_hostname(mysql_host)
            .user(Some(mysql_user))
            .pass(Some(mysql_pwd))
            .db_name(Some(mysql_dbname))
            .pool_opts(pool_opts),
    );
    let pool = mysql_pool.clone();
    let mut conn = pool.get_conn().await.unwrap();

    // ============================
    // 5. MySQL query: parent nodes
    // ============================
    let mut parent_node: Vec<Uuid> = vec![];
    // ===============================
    // SQL for test data (Films only): fetch all Film nodes using the sortk value
    //let child_edge = r#"select distinct puid from test_childedge where sortk = "m|A#G#:G""#
    // ===============================
    //let _parent_edge = "SELECT Uid FROM Edge_test order by cnt desc"
    let _parent_edge = r#"select distinct puid from test_childedge where sortk = "m|A#G#:G""#
        .with(())
        .map(&mut conn, |puid| parent_node.push(puid))
        .await?;
    // =======================================================
    // 6. MySQL query: load all parent node edges into memory (TODO: batch query)
    // =======================================================
    println!("About to SQL");
    let mut parent_edges: HashMap<Puid, HashMap<SortK, Vec<Cuid>>> = HashMap::new();

    let _child_edge = "Select puid,sortk,cuid from test_childedge order by puid,sortk"
        .with(())
        .map(&mut conn, |(puid, sortk, cuid): (Uuid, String, Uuid)| {
            // this version requires no allocation (cloning) of sortk
            match parent_edges.get_mut(&puid) {
                None => {
                    let mut e = HashMap::new();
                    e.insert(sortk, vec![cuid]);
                    parent_edges.insert(puid, e);
                }
                Some(e) => match e.get_mut(&sortk[..]) {
                    None => {
                        let e = match parent_edges.get_mut(&puid) {
                            None => {
                                panic!("logic error in parent_edges get_mut()");
                            }
                            Some(e) => e,
                        };
                        e.insert(sortk, vec![cuid]);
                    }
                    Some(c) => {
                        c.push(cuid);
                    }
                },
            }
        })
        .await?;
    println!("About to SQL - DONE");
    let start_1: Instant = Instant::now();
    // ===========================================
    // 7. Setup asynchronous tasks infrastructure
    // ===========================================
    let mut tasks: usize = 0;
    let (prod_ch, mut task_rx) = tokio::sync::mpsc::channel::<bool>(max_sp_tasks);
    // ====================================
    // 8. Setup retry failed writes channel
    // ====================================
    let (retry_send_ch, _retry_rx) =
        tokio::sync::mpsc::channel::<Vec<aws_sdk_dynamodb::types::WriteRequest>>(max_sp_tasks);
    // ===============================================================================
    // 9. process each parent_node and its associated edges (child nodes) in parallel
    // ===============================================================================
    for puid in parent_node {
        //println!("puid {}",puid);
        // if puid.to_string() != "0eb40290-da22-4619-91d2-80e708eb4abb" {//"0abc72f2-79c2-4af5-b7f4-38eefb77618d" {// 5d14c8b4-43e4-4a6b-8f0a-5cd7f1c2d9b3" { // || puid.to_string() = { // a Peter Sellers Performance node 8ce42327-0183-4632-9ba8-065808909144
        //     continue
        // }

        // println!("puid  [{}]", puid.to_string());
        // ------------------------------------------
        let p_sk_edges = match parent_edges.remove(&puid) {
            None => {
                panic!("logic error. No entry found in parent_edges");
            }
            Some(e) => e,
        };
        // =====================================================
        // 9.1 clone enclosed vars before moving into task block
        // =====================================================
        let task_ch = prod_ch.clone();
        let dyn_client = dynamo_client.clone();
        let retry_ch = retry_send_ch.clone();
        let graph_sn = graph_prefix_wdot.trim_end_matches('.').to_string();
        let node_types = node_types.clone(); // Arc instance - single cache in heap storage
        let cache = reverse_edge_cache.clone();
        let waits = waits.clone();

        tasks += 1; // concurrent task counter
        task += 1;

        // =========================================
        // 9.2 spawn tokio task for each parent node
        // =========================================
        tokio::spawn(async move {
            // ============================================
            // 9.2.3 propagate child scalar data to parent
            // ============================================
            println!(
                "********************** MAIN TASK {} [{}] ******************************",
                task, tasks
            );

            for (p_sk_edge, children) in p_sk_edges {
                // Container for Overflow Block Uuids, also stores all propagated data.
                let mut ovb_pk: HashMap<String, Vec<Uuid>> = HashMap::new();
                let mut items: HashMap<SortK, Operation> = HashMap::new();

                //println!("edge {}  children: {}", p_sk_edge, children.len());
                // =====================================================================
                // p_node_ty : find type of puid . use sk "m|T#"  <graph>|<T,partition># //TODO : type short name should be in mysql table - saves fetching here.
                // =====================================================================
                let (p_node_ty, ovbs) = fetch_p_edge_meta(
                    &dyn_client,
                    &puid,
                    &p_sk_edge,
                    &graph_sn,
                    &node_types,
                    table_name,
                    waits.clone(),
                )
                .await;
                // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                ovb_pk.insert(p_sk_edge.clone(), ovbs);

                let p_edge_attr_sn = &p_sk_edge[p_sk_edge.rfind(':').unwrap() + 1..]; // A#G#:A -> "A"

                let p_edge_attr_nm = p_node_ty.get_attr_nm(p_edge_attr_sn);
                let add_rvs_edge = p_node_ty.add_rvs_edge(p_edge_attr_nm);
                let child_ty = node_types.get(p_node_ty.get_edge_child_ty(p_edge_attr_nm));
                //let child_ty = node_types.get(p_node_ty.get_edge_child_ty(p_edge_attr_sn));
                //let add_rvs_edge = child_ty.get_add_rvs_edge(p_edge_attr_sn);
                let child_scalar_attr = child_ty.get_scalars();
                // check if child type has defined an edge back to parent - if not add a reverse edge to child

                // reverse edge item : R#<parent-node-type-sn>#:edge_sk (R#P#:D) as saved in child node
                let reverse_sk: String =
                    "R#".to_string() + p_node_ty.short_nm() + "#:" + &p_edge_attr_sn;
                // ===================================================================
                // 9.2.3.0 query on p_node edge and get OvBs from Nd attribute of edge
                // ===================================================================
                let mut bat_w_req: Vec<WriteRequest> = vec![];

                for cuid in children {
                    //let cuid_p = cuid.clone();
                    // =====================================================================
                    // 9.2.3.1 for each child node's scalar partitions and scalar attributes
                    // =====================================================================
                    for (partition, attrs) in &child_scalar_attr {
                        let mut sk_query = graph_sn.clone(); // generate sortk's for query
                        sk_query.push_str("|A#");
                        sk_query.push_str(&partition);

                        if attrs.len() == 1 {
                            sk_query.push_str("#:");
                            sk_query.push_str(attrs[0]);
                        }

                        // ============================================================
                        // 9.2.3.1.1 fetch child node scalar data by sortk partition
                        // ============================================================
                        let result = dyn_client
                            .query()
                            .table_name(table_name)
                            .key_condition_expression("#p = :uid and begins_with(#s,:sk_v)")
                            .expression_attribute_names("#p", types::PK)
                            .expression_attribute_names("#s", types::SK)
                            .expression_attribute_values(
                                ":uid",
                                AttributeValue::B(Blob::new(cuid.clone())),
                            )
                            .expression_attribute_values(":sk_v", AttributeValue::S(sk_query))
                            .send()
                            .await;

                        if let Err(err) = result {
                            panic!("error in query() {}", err);
                        }
                        // ============================================================
                        // 9.2.3.1.2 populate node cach (nc) from query result
                        // ============================================================
                        let mut nc: Vec<types::DataItem> = vec![];
                        let mut nc_attr_map: types::NodeCache = types::NodeCache(HashMap::new()); // HashMap<types::AttrShortNm, types::DataItem> = HashMap::new();

                        if let Some(dyn_items) = result.unwrap().items {
                            nc = dyn_items.into_iter().map(|v| v.into()).collect();
                        }

                        for c in nc {
                            nc_attr_map.0.insert(c.sk.attribute_sn().to_owned(), c);
                        }
                        // ===============================================================================
                        // 9.2.3.1.3 add scalar data for each attribute queried above to edge in items
                        // ===============================================================================
                        for &attr_sn in attrs {
                            // associated parent node sort key to attach child's scalar data
                            // generate sk for propagated (ppg) data
                            let mut ppg_sk = p_sk_edge.clone();
                            // ppg_sk.push('#');
                            // ppg_sk.push_str(partition.as_str());
                            ppg_sk.push_str("#:");
                            ppg_sk.push_str(attr_sn);

                            //let dt = ty_c.get_attr_dt(child_ty,attr_sn);
                            let dt = child_ty.get_attr_dt(attr_sn);
                            // check if ppg_sk in query cache
                            let op_ppg = match items.get_mut(&ppg_sk[..]) {
                                None => {
                                    let op = Operation::Propagate(PropagateScalar {
                                        entry: None,
                                        psk: p_sk_edge.clone(), // parent edge
                                        sk: ppg_sk.clone(),     // child propagated scalar
                                        ls: vec![],
                                        ln: vec![],
                                        lbl: vec![],
                                        lb: vec![],
                                        ldt: vec![],
                                        cuids: vec![],
                                    });
                                    items.insert(ppg_sk.clone(), op);
                                    items.get_mut(&ppg_sk[..]).unwrap()
                                }
                                Some(es) => es,
                            };

                            let e_p = match op_ppg {
                                Operation::Propagate(ref mut e_) => e_,
                                _ => {
                                    panic!("Expected Operation::Propagate")
                                }
                            };

                            let Some(di) = nc_attr_map.0.remove(attr_sn) else {
                                panic!("not found in nc_attr_map [{}]", ppg_sk)
                            };
                            //println!("query cache  attr_sn [{}]   ppg_sk  [{}] di.sk [{:?}] dt {}",attr_sn,ppg_sk, di.sk, dt);

                            match dt {
                                "S" => {
                                    match di.s {
                                        None => {
                                            if !child_ty.is_atttr_nullable(attr_sn) {
                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                            }
                                            e_p.ls.push(AttributeValue::Null(true));
                                        }
                                        Some(v) => {
                                            e_p.ls.push(AttributeValue::S(v));
                                            // e_p.cuids.push(AttributeValue::B(Blob::new(cuid)));
                                            e_p.cuids.push(cuid);
                                        }
                                    }
                                    e_p.entry = Some(LS);
                                }

                                "I" | "F" => {
                                    match di.n {
                                        // no conversion into int or float. Keep as String for propagation purposes.
                                        None => {
                                            if !child_ty.is_atttr_nullable(attr_sn) {
                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                            }
                                            e_p.ln.push(AttributeValue::Null(true));
                                        }
                                        Some(v) => {
                                            e_p.ln.push(AttributeValue::N(v));
                                            //e_p.cuids.push(AttributeValue::B(Blob::new(cuid)));
                                            e_p.cuids.push(cuid);
                                        }
                                    }
                                    e_p.entry = Some(LN);
                                }

                                "B" => {
                                    match di.b {
                                        None => {
                                            if !child_ty.is_atttr_nullable(attr_sn) {
                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                            }
                                            e_p.lb.push(AttributeValue::Null(true));
                                        }
                                        Some(v) => e_p.lb.push(AttributeValue::B(Blob::new(v))),
                                    }
                                    e_p.entry = Some(LB);
                                }
                                //"DT" => e_p.ldt.push(AttributeValue::S(di.dt)),
                                "Bl" => {
                                    match di.bl {
                                        None => {
                                            if !child_ty.is_atttr_nullable(attr_sn) {
                                                panic!("Data Error: Attribute {} in type {} is not null but null returned from db",attr_sn,child_ty.long_nm())
                                            }
                                            e_p.lbl.push(AttributeValue::Null(true));
                                        }
                                        Some(v) => {
                                            e_p.lbl.push(AttributeValue::Bool(v));
                                            //e_p.cuids.push(AttributeValue::B(Blob::new(cuid)));
                                            e_p.cuids.push(cuid);
                                        }
                                    }
                                    e_p.entry = Some(LBL);
                                }

                                _ => {
                                    panic!("expected Scalar Type, got [{}]", dt)
                                }
                            }
                        }
                    }
                }
                // ======================================================
                // 9.2.3 persist parent nodes propagated data to database
                // ======================================================
                persist(
                    task,
                    &dyn_client,
                    table_name,
                    cache.clone(),
                    bat_w_req,
                    add_rvs_edge,
                    puid,
                    reverse_sk,
                    retry_ch.clone(),
                    ovb_pk,
                    items,
                    //
                    waits.clone(),
                )
                .await;
            }
            // ===================================
            // 9.2.4 send complete message to main
            // ===================================
            let _ = task_ch.send(true).await;
        });
        // =============================
        // 9.3 Wait for task to complete
        // =============================
        if tasks == max_sp_tasks {
            task_rx.recv().await;
            tasks -= 1;
        }
    }
    // =========================================
    // 10.0 Wait for remaining tasks to complete
    // =========================================
    while tasks > 0 {
        // wait for a task to finish...
        task_rx.recv().await;
        tasks -= 1;
    }
    // task channel should be empty
    assert!(task_rx.is_empty());
    println!(
        "MAIN: Duration of SP: {:?}",
        Instant::now().duration_since(start_1)
    );
    //let _ = lru_service.await;
    // ==============
    // Shutdown cache
    // ==============
    println!("MAIN: shutdown cache");
    reverse_edge_cache.shutdown().await;

    // ==============================
    // Shutdown other services
    // ==============================
    println!("MAIN: shutdown services");
    let _ = shutdown_broadcast_sender.send(0);

    println!("MAIN: stats_service.await;");
    let _ = stats_service.await;

    println!("MAIN: retry_service.await;");
    let _ = retry_service.await;
    println!("MAIN: EXIT ");

    Ok(())
}

async fn persist(
    task: usize,
    dyn_client: &DynamoClient,
    table_name: &str, //
    cache: Cache<RKey, RNode>, //
    mut bat_w_req: Vec<WriteRequest>,
    add_rvs_edge: bool, //
    target_uid: Uuid,
    reverse_sk: String, //
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    ovb_pk: HashMap<String, Vec<Uuid>>,
    items: HashMap<SortK, Operation>, //
    _waits: event_stats::Waits,
) {
    // create channels to communicate (to and from) lru eviction service
    // evict_resp_ch: sender - passed to eviction service so it can send its response back to this routine
    // evict_recv_ch: receiver - used by this routine to receive respone from eviction service
    // persist to database
    for (sk, v) in items {
        match v {
            Operation::Attach(_) => {}
            Operation::Propagate(mut e) => {
                let mut finished = false;

                let put = aws_sdk_dynamodb::types::PutRequest::builder();
                let put = put
                    .item(types::PK, AttributeValue::B(Blob::new(target_uid.clone())))
                    .item(types::SK, AttributeValue::S(sk.clone()));
                let mut put = match ovb_pk.get(&e.psk) {
                    None => {
                        panic!("Logic error: no key found in ovb_pk for {}", e.psk)
                    }
                    Some(v) => match v.len() {
                        0 => put.item(types::OVB, AttributeValue::Bool(false)),
                        _ => put.item(types::OVB, AttributeValue::Bool(true)),
                    },
                };

                let children = match e.entry.unwrap() {
                    LS => {
                        if e.ls.len() <= EMBEDDED_CHILD_NODES {
                            let children = mem::take(&mut e.cuids);
                            let embedded: Vec<_> = std::mem::take(&mut e.ls);
                            put = put.item(types::LS, AttributeValue::L(embedded));
                            finished = true;
                            children
                        } else {
                            let mut children = e.cuids.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut children, &mut e.cuids);
                            let mut embedded = e.ls.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut embedded, &mut e.ls);
                            put = put.item(types::LS, AttributeValue::L(embedded));
                            children
                        }
                    }
                    LN => {
                        if e.ln.len() <= EMBEDDED_CHILD_NODES {
                            let children = mem::take(&mut e.cuids);
                            let embedded: Vec<_> = std::mem::take(&mut e.ln);
                            put = put.item(types::LN, AttributeValue::L(embedded));
                            finished = true;
                            children
                        } else {
                            let mut children = e.cuids.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut children, &mut e.cuids);
                            let mut embedded = e.ln.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut embedded, &mut e.ln);
                            put = put.item(types::LN, AttributeValue::L(embedded));
                            children
                        }
                    }
                    LBL => {
                        if e.lbl.len() <= EMBEDDED_CHILD_NODES {
                            let children = mem::take(&mut e.cuids);
                            let embedded: Vec<_> = std::mem::take(&mut e.lbl);
                            put = put.item(types::LBL, AttributeValue::L(embedded));
                            finished = true;
                            children
                        } else {
                            let mut children = e.cuids.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut children, &mut e.cuids);
                            let mut embedded = e.lbl.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut embedded, &mut e.lbl);
                            put = put.item(types::LBL, AttributeValue::L(embedded));
                            children
                        }
                    }
                    LB => {
                        if e.lb.len() <= EMBEDDED_CHILD_NODES {
                            let children = mem::take(&mut e.cuids);
                            let embedded: Vec<_> = std::mem::take(&mut e.lb);
                            put = put.item(types::LB, AttributeValue::L(embedded));
                            finished = true;
                            children
                        } else {
                            let mut children = e.cuids.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut children, &mut e.cuids);
                            let mut embedded = e.lbl.split_off(EMBEDDED_CHILD_NODES);
                            std::mem::swap(&mut embedded, &mut e.lbl);
                            put = put.item(types::LB, AttributeValue::L(embedded));
                            children
                        }
                    }
                    _ => {
                        panic!("unexpected entry match in Operation::Propagate")
                    }
                };

                bat_w_req =
                    save_item(&dyn_client, bat_w_req, retry_ch.clone(), put, table_name).await;

                if add_rvs_edge {
                    for (id, child) in children.into_iter().enumerate() {
                        let rkey = RKey::new(child.clone(), reverse_sk.clone());
                        rkey.add_reverse_edge(
                            task,
                            dyn_client,
                            table_name, //
                            &cache, 
                            &target_uid,
                            0,
                            id,
                        )
                        .await;
                    }
                }

                if finished {
                    continue;
                }

                // =========================================
                // add batches across ovbs until max reached
                // =========================================
                let mut bid: usize = 0;

                for ovb in ovb_pk.get(&e.psk).unwrap() {
                    bid = 0;

                    while bid < OV_BATCH_THRESHOLD && !finished {
                        bid += 1;
                        let mut sk_w_bid = sk.clone();
                        sk_w_bid.push('%');
                        sk_w_bid.push_str(&bid.to_string());

                        let put = aws_sdk_dynamodb::types::PutRequest::builder();
                        let mut put = put
                            .item(types::PK, AttributeValue::B(Blob::new(ovb.clone())))
                            .item(types::SK, AttributeValue::S(sk_w_bid));

                        let children = match e.entry.unwrap() {
                            LS => {
                                if e.ls.len() <= OV_MAX_BATCH_SIZE { 
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.ls);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    finished = true;
                                    // children = e.cuids.drain(..e.cuids.len()).collect();
                                    // let batch: Vec<_> = e.ls.drain(..e.ls.len()).collect();
                                    // put = put.item(types::LS, AttributeValue::L(batch));
                                    // finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.ls.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.ls);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    // children = e.cuids.drain(..OV_MAX_BATCH_SIZE).collect();
                                    // let batch: Vec<_> = e.ls.drain(..OV_MAX_BATCH_SIZE).collect();
                                    // put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LN => {
                                if e.ln.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.ln);
                                    put = put.item(types::LN, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.ln.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.ln);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LBL => {
                                if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.lbl);
                                    put = put.item(types::LBL, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.lbl.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.lbl);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LB => {
                                if e.lb.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.lb);
                                    put = put.item(types::LB, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.lb.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.lb);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }
                            _ => {
                                panic!("unexpected entry match in Operation::Propagate")
                            }
                        };

                        bat_w_req =
                            save_item(&dyn_client, bat_w_req, retry_ch.clone(), put, table_name)
                                .await;

                        if add_rvs_edge {
                            for (id, child) in children.into_iter().enumerate() {
                                let rkey = RKey::new(child.clone(), reverse_sk.clone());
                                rkey.add_reverse_edge(
                                    task,
                                    dyn_client,
                                    table_name, //
                                    &cache, 
                                    &ovb,
                                    bid,
                                    id, 
                                )
                                .await;
                            }
                        }
                    }
                    if finished {
                        break;
                    }
                }
                // =============================================
                // keep adding batches across ovbs (round robin)
                // =============================================
                while !finished {
                    bid += 1;

                    for ovb in ovb_pk.get(&e.psk).unwrap() {
                        let mut sk_w_bid = sk.clone();
                        sk_w_bid.push('%');
                        sk_w_bid.push_str(&bid.to_string());
                        let put = aws_sdk_dynamodb::types::PutRequest::builder();
                        let mut put = put
                            .item(types::PK, AttributeValue::B(Blob::new(ovb.clone())))
                            .item(types::SK, AttributeValue::S(sk_w_bid));

                        let children = match e.entry.unwrap() {
                            LS => {
                                if e.ls.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.ls);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.ls.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.ls);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LN => {
                                if e.ln.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.ln);
                                    put = put.item(types::LN, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.ln.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.ln);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LBL => {
                                if e.lbl.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.lbl);
                                    put = put.item(types::LBL, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.lbl.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.lbl);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }

                            LB => {
                                if e.lb.len() <= OV_MAX_BATCH_SIZE {
                                    let children = mem::take(&mut e.cuids);
                                    let batch: Vec<_> = std::mem::take(&mut e.lb);
                                    put = put.item(types::LB, AttributeValue::L(batch));
                                    finished = true;
                                    children
                                } else {
                                    let mut children = e.cuids.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut children, &mut e.cuids);
                                    let mut batch = e.lb.split_off(OV_MAX_BATCH_SIZE);
                                    std::mem::swap(&mut batch, &mut e.lb);
                                    put = put.item(types::LS, AttributeValue::L(batch));
                                    children
                                }
                            }
                            _ => {
                                panic!("unexpected entry match in Operation::Propagate")
                            }
                        };

                        bat_w_req =
                            save_item(&dyn_client, bat_w_req, retry_ch.clone(), put, table_name)
                                .await;

                        if add_rvs_edge {
                            for (id, child) in children.into_iter().enumerate() {
                                // below design makes use of Mutexes to serialise access to cache
                                // alternatively, manage addition of reverse edges via a "service" or separate load process.
                                let rkey = RKey::new(child.clone(), reverse_sk.clone());
                                rkey.add_reverse_edge(
                                    task,
                                    dyn_client,
                                    table_name, //
                                    &cache, 
                                    &ovb,
                                    bid,
                                    id  ,
                                )
                                .await;
                            } //unlock cache and edgeItem locks
                        }

                        if e.ls.len() == 0 && e.ln.len() == 0 && e.ln.len() == 0 && e.lb.len() == 0
                        {
                            finished = true;
                            break;
                        }
                    }
                }
            }
        } // end match

        if bat_w_req.len() > 0 {
            //print_batch(bat_w_req);
            bat_w_req =
                persist_dynamo_batch(dyn_client, bat_w_req, retry_ch.clone(), table_name).await;
        }
    } // end for

    if bat_w_req.len() > 0 {
        //print_batch(bat_w_req);
        _ = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch.clone(), table_name).await;
    }
}

//static LOAD_PROJ : LazyLock<String> = LazyLock::new(||types::OVB_s ) + "," + types::OVB_BID + "," + types::OVB_ID + "," + types::OVB_CUR;
// static LOAD_PROJ: LazyLock<String> = LazyLock::new(|| {
//     types::OVB.to_string() + "," + types::OVB_BID + "," + types::OVB_ID + "," + types::OVB_CUR
// });

// returns node type as String, moving ownership from AttributeValue - preventing further allocation.
async fn fetch_p_edge_meta<'a, T: Into<String>>(
    dyn_client: &DynamoClient,
    uid: &Uuid,
    sk: &str,
    _graph_sn: T,
    node_types: &'a types::NodeTypes,
    table_name: &str,
    waits: event_stats::Waits,
) -> (&'a types::NodeType, Vec<Uuid>) {
    let proj = types::ND.to_owned() + "," + types::XF + "," + types::TY;

    let before = Instant::now();
    let result = dyn_client
        .get_item()
        .table_name(table_name)
        .key(types::PK, AttributeValue::B(Blob::new(uid.clone())))
        .key(types::SK, AttributeValue::S(sk.to_owned()))
        .projection_expression(proj)
        .send()
        .await;
    waits
        .record(
            event_stats::Event::GetItem,
            Instant::now().duration_since(before),
        )
        .await;

    if let Err(err) = result {
        panic!(
            "get node type: no item found: expected a type value for node. Error: {}",
            err
        )
    }
    let di: types::DataItem = match result.unwrap().item {
        None => panic!(
            "No type item found in fetch_node_type() for [{}] [{}]",
            uid, sk
        ),
        Some(v) => v.into(),
    };

    let ovb_cnt = di
    .xf
    .as_ref()
    .expect("xf is None")
    .iter()
    .filter(|&&v| v == 4)
    .fold(0, |a, _| a + 1); // xf idx entry of first Ovb Uuid
    if ovb_cnt > MAX_OV_BLOCKS {
        panic!(
            "OvB inconsistency: XF overflow blcoks {} exceeeds MAX_OV_BLOCKS {}",
            ovb_cnt, MAX_OV_BLOCKS
        );
    }

    let ovb_start_idx = di
                .xf
                .as_ref()
                .expect("xf is None")
                .iter()
                .filter(|&&v| v < 4)
                .fold(0, |a, _| a + 1); // xf idx entry of first Ovb Uuid
    if ovb_cnt > 0 && ovb_start_idx != EMBEDDED_CHILD_NODES {
                panic!(
                    "OvB inconsistency: XF embedded entry {} does not match EMBEDDED_CHILD_NODES {}",
                    ovb_start_idx, EMBEDDED_CHILD_NODES
                );
    }


    let ovb_pk: Vec<Uuid> = di.nd.expect("nd is None").drain(ovb_start_idx..).collect(); //TODO:consider split_off + mem::swap
                                                                                         // let mut ovb_pk: Vec<Uuid> = di.nd.as_mut().expect("nd is None").split_off(ovb_start_idx);
                                                                                         // mem::swap(&mut ovb_pk,  &mut di.nd.unwrap());

    (node_types.get(&di.ty.expect("ty is None")), ovb_pk)
}

async fn save_item(
    dyn_client: &DynamoClient,
    mut bat_w_req: Vec<WriteRequest>,
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    put: PutRequestBuilder,
    table_name: &str,
) -> Vec<WriteRequest> {
    match put.build() {
        Err(err) => {
            println!("error in write_request builder: {}", err);
        }
        Ok(req) => {
            bat_w_req.push(WriteRequest::builder().put_request(req).build());
        }
    }
    //bat_w_req = print_batch(bat_w_req);

    if bat_w_req.len() == DYNAMO_BATCH_SIZE {
        // =================================================================================
        // persist to Dynamodb
        bat_w_req = persist_dynamo_batch(dyn_client, bat_w_req, retry_ch, table_name).await;
        // =================================================================================
        //bat_w_req = print_batch(bat_w_req);
    }
    bat_w_req
}

async fn persist_dynamo_batch(
    dyn_client: &DynamoClient,
    bat_w_req: Vec<WriteRequest>,
    retry_ch: tokio::sync::mpsc::Sender<Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    table_name: &str,
) -> Vec<WriteRequest> {
    let bat_w_outp = dyn_client
        .batch_write_item()
        .request_items(table_name, bat_w_req)
        .send()
        .await;

    match bat_w_outp {
        Err(err) => {
            panic!(
                "Error in Dynamodb batch write in persist_dynamo_batch() - {}",
                err
            );
        }
        Ok(resp) => {
            if resp.unprocessed_items.as_ref().unwrap().values().len() > 0 {
                // send unprocessed writerequests on retry channel
                for (_, v) in resp.unprocessed_items.unwrap() {
                    println!("persist_dynamo_batch, unprocessed items..delay 2secs");
                    sleep(Duration::from_millis(2000)).await;
                    let resp = retry_ch.send(v).await; // retry_ch auto deref'd to access method send.

                    if let Err(err) = resp {
                        panic!("Error sending on retry channel : {}", err);
                    }
                }

                // TODO: aggregate batchwrite metrics in bat_w_output.
                // pub item_collection_metrics: Option<HashMap<String, Vec<ItemCollectionMetrics>>>,
                // pub consumed_capacity: Option<Vec<ConsumedCapacity>>,
            }
        }
    }
    let mut new_bat_w_req: Vec<WriteRequest> = vec![];
    new_bat_w_req
}

//fn print_batch(bat_w_req: Vec<WriteRequest>) -> Vec<WriteRequest> {
    // for r in bat_w_req {
    //     let WriteRequest {
    //         put_request: pr, ..
    //     } = r;
    //     println!(" ------------------------  ");
    //     for (attr, attrval) in pr.unwrap().item {
    //         // HashMap<String, AttributeValue>,
    //         println!(" putRequest [{}]   {:?}", attr, attrval);
    //     }
    // }

//     let new_bat_w_req: Vec<WriteRequest> = vec![];

//     new_bat_w_req
// }
