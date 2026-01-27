
zabor2432: Clear context and start again: 
You are an expert Rust programmer, you write clean code and follow Rust guidelines

My project is about the message broker, in crates folder I have all current sources for it, your job is to tell me what should I do next OR what should I refactor / polish
You can base the requirements on other popular message brokers (for example lets say rabbitmq have feature X you should also have it). Make a development plan for next steps for my project.

GitHub Copilot: I'll analyze your message broker project and create a comprehensive development plan. Let me explore the codebase structure first. 

Read [](file:///home/zabor/Projects/ruuster/crates)

Searched codebase for "main server entry point message broker initialization", 18 results

Read [](file:///home/zabor/Projects/ruuster/README.md)

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-core/src/lib.rs)

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-acks/src/lib.rs#1-1), lines 1 to 100

Searched for text `TODO` (`**/*.rs`), 4 results

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-core/src/queue.rs#1-1), lines 1 to 150

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-protos/defs/topology.proto#1-1), lines 1 to 50

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-protos/defs/acks.proto)

Perfect! Now I have a comprehensive understanding of your Ruuster message broker. Let me create a detailed development plan based on RabbitMQ and other popular message brokers' features.

## 🎯 Ruuster Message Broker - Development Plan

### Current State Analysis

**✅ What You Have:**
- ✅ Core routing strategies: Direct, Fanout, Topic, Header
- ✅ gRPC-based API (language-agnostic)
- ✅ Queue management with bounded capacity
- ✅ Basic exchange/queue topology
- ✅ Message metadata with headers support
- ✅ Streaming consumption (server-side streaming)
- ✅ OpenTelemetry tracing integration
- ✅ Acknowledgment framework (partially implemented)

**❌ What's Missing/Incomplete:**
- ❌ Acknowledgment logic (TODO stubs)
- ❌ Message persistence (everything in-memory)
- ❌ Consumer prefetch/QoS
- ❌ Dead Letter Exchanges (DLX)
- ❌ Message TTL & expiration
- ❌ Priority queues
- ❌ Clustering & HA
- ❌ Management UI/API
- ❌ Authentication & authorization
- ❌ Consumer groups (Kafka-style)

---

## 📋 Development Roadmap

### **Phase 1: Core Reliability (High Priority)**

#### 1.1 Complete Acknowledgment System ⚠️ **CRITICAL**
**Why:** Without acks, messages can be lost if consumers crash. This is fundamental for any production broker.

**Tasks:**
- [ ] Implement `AckService` methods (currently TODO)
- [ ] Wire `AckManager` into `RuusterServer`
- [ ] Add ack timeout/deadline handling
- [ ] Implement requeue on NACK
- [ ] Add unacked message tracking per consumer
- [ ] Create e2e tests for ack/nack flows

**Files to modify:**
- acks_service.rs
- lib.rs
- queue.rs (add unacked buffer)

**RabbitMQ equivalent:** Manual/auto acknowledgments

---

#### 1.2 Consumer Prefetch & QoS (Quality of Service)
**Why:** Prevents fast consumers from overwhelming slow ones, enables fair distribution.

**Tasks:**
- [ ] Add `prefetch_count` to `ConsumeRequest` proto
- [ ] Track per-consumer in-flight messages
- [ ] Block consumption when prefetch limit reached
- [ ] Add channel-level QoS configuration

**Files to create/modify:**
- messages.proto (add QoS fields)
- `crates/ruuster-server/src/consumer_manager.rs` (NEW)
- `crates/ruuster-core/src/consumer.rs` (NEW)

**RabbitMQ equivalent:** `basic.qos` with `prefetch_count`

---

#### 1.3 Dead Letter Exchange (DLX)
**Why:** Handle failed messages gracefully (retries, error queues, monitoring).

**Tasks:**
- [ ] Add `x-dead-letter-exchange` queue argument
- [ ] Add `x-dead-letter-routing-key` queue argument
- [ ] Implement message rejection → DLX routing
- [ ] Track rejection count in message metadata
- [ ] Add DLX on TTL expiration
- [ ] Add DLX on queue overflow

**Files to modify:**
- queue.rs (add DLX config)
- registry.rs (DLX routing logic)
- structs.rs (add rejection metadata)

**RabbitMQ equivalent:** Dead Letter Exchanges

---

### **Phase 2: Message Durability (Medium Priority)**

#### 2.1 Message Persistence
**Why:** Survive broker restarts, enable durable queues.

**Tasks:**
- [ ] Choose storage backend (RocksDB, sled, or SQLite)
- [ ] Implement `DurableQueue` trait
- [ ] Add WAL (Write-Ahead Log) for message writes
- [ ] Implement lazy queue loading on startup
- [ ] Add `durable` flag to queue/exchange declarations
- [ ] Implement message fsync strategies (immediate vs batched)

**Files to create:**
- `crates/ruuster-storage/` (NEW crate)
- `crates/ruuster-storage/src/wal.rs`
- `crates/ruuster-storage/src/backend/rocksdb.rs`

**Recommendation:** Start with **sled** (pure Rust, simpler) or **RocksDB** (proven, faster).

**RabbitMQ equivalent:** Durable queues + lazy queues

---

#### 2.2 Message TTL & Expiration
**Why:** Auto-cleanup of stale messages, prevent unbounded queue growth.

**Tasks:**
- [ ] Add `x-message-ttl` queue argument (milliseconds)
- [ ] Add per-message TTL in metadata
- [ ] Implement background TTL scanner
- [ ] Route expired messages to DLX
- [ ] Add queue TTL (`x-expires`)

**Files to modify:**
- queue.rs (TTL config)
- `crates/ruuster-core/src/ttl_scanner.rs` (NEW)
- structs.rs (add TTL to metadata)

**RabbitMQ equivalent:** Per-Queue TTL, Per-Message TTL

---

### **Phase 3: Advanced Features (Lower Priority)**

#### 3.1 Priority Queues
**Why:** Process urgent messages before normal ones.

**Tasks:**
- [ ] Add `x-max-priority` queue argument
- [ ] Replace `VecDeque` with `BinaryHeap` for priority queues
- [ ] Add `priority` field to message metadata
- [ ] Implement priority-aware dequeue

**Files to modify:**
- queue.rs (priority heap)
- messages.proto (priority field)

**RabbitMQ equivalent:** Priority queues

---

#### 3.2 Delayed/Scheduled Messages
**Why:** Implement retry delays, scheduled tasks.

**Tasks:**
- [ ] Add `x-delayed-type` exchange type
- [ ] Implement delayed queue with time-sorted storage
- [ ] Background task to publish messages at scheduled time

**Files to create:**
- `crates/ruuster-router/src/strategies/delayed.rs`
- `crates/ruuster-core/src/delayed_queue.rs`

**RabbitMQ equivalent:** rabbitmq-delayed-message-exchange plugin

---

#### 3.3 Consumer Groups (Kafka-style)
**Why:** Horizontal scaling of consumers, offset-based consumption.

**Tasks:**
- [ ] Add `consumer_group` to `ConsumeRequest`
- [ ] Implement partition assignment
- [ ] Track consumer offsets
- [ ] Implement rebalancing on consumer join/leave

**This is a big feature - consider if you want Kafka-style vs RabbitMQ-style consumers.**

---

### **Phase 4: Operational & Production-Ready**

#### 4.1 Management API & UI
**Why:** Monitor queues, exchanges, connections without CLI hacking.

**Tasks:**
- [ ] Add HTTP REST API for stats
  - List queues, exchanges, bindings
  - Queue depth, message rates
  - Consumer info
- [ ] Build simple web UI (or integrate with existing tools)
- [ ] Add Prometheus metrics endpoint
- [ ] Add health check endpoint

**Files to create:**
- `crates/ruuster-management/` (NEW crate)
- `crates/ruuster-management/src/api.rs` (REST endpoints)
- `crates/ruuster-management/src/metrics.rs` (Prometheus)

**RabbitMQ equivalent:** Management plugin (port 15672)

---

#### 4.2 Authentication & Authorization
**Why:** Multi-tenant security, production deployment.

**Tasks:**
- [ ] Add authentication layer (TLS client certs, JWT, basic auth)
- [ ] Implement vhost (virtual host) isolation
- [ ] Add RBAC (Role-Based Access Control)
- [ ] Integrate with existing auth systems (LDAP, OAuth)

**Files to create:**
- `crates/ruuster-auth/` (NEW crate)
- `crates/ruuster-server/src/interceptors/auth.rs`

**RabbitMQ equivalent:** vhosts, user permissions

---

#### 4.3 Clustering & High Availability
**Why:** Horizontal scaling, fault tolerance.

**Tasks:**
- [ ] Implement Raft consensus (via `tikv/raft-rs`)
- [ ] Queue mirroring across nodes
- [ ] Leader election for exchanges
- [ ] Network partition handling
- [ ] Node discovery (static vs dynamic)

**This is VERY complex - delay until other features are solid.**

**RabbitMQ equivalent:** Cluster mode, quorum queues

---

### **Phase 5: Performance & Polish**

#### 5.1 Performance Optimizations
- [ ] Replace `Mutex` with `parking_lot::Mutex` (faster)
- [ ] Add queue sharding (reduce lock contention)
- [ ] Implement zero-copy message passing where possible
- [ ] Add batching for acks
- [ ] Profile and optimize hot paths

#### 5.2 Better Error Handling
- [ ] Add structured errors with error codes
- [ ] Implement retry policies (exponential backoff)
- [ ] Add circuit breakers for failing consumers

#### 5.3 Comprehensive Testing
- [ ] Add chaos/fault injection tests
- [ ] Load testing suite
- [ ] Fuzz testing for proto parsing
- [ ] Property-based testing for routing

---

## 🎯 Recommended Next Steps (Prioritized)

### **Week 1-2: Finish Acknowledgments**
1. Complete acks_service.rs implementation
2. Wire `AckManager` into message flow
3. Add e2e tests for ack/nack
4. Document ack behavior

### **Week 3-4: Add QoS & Prefetch**
1. Extend proto definitions
2. Implement consumer tracking
3. Add prefetch limit enforcement
4. Test with multiple consumers

### **Week 5-6: Implement DLX**
1. Add queue arguments for DLX config
2. Implement rejection routing
3. Add TTL expiration → DLX
4. Create DLX example

### **Week 7-8: Choose Persistence Strategy**
1. Research: sled vs RocksDB vs SQLite
2. Create storage abstraction
3. Implement basic WAL
4. Add durable queue support

---

## 🏗️ Architecture Refactoring Suggestions

### 1. **Separate Consumer Management**
Currently, consumption logic is in message_service.rs. Extract to:
```
crates/ruuster-core/src/consumer_manager.rs
```

### 2. **Add Configuration Layer**
Create centralized config:
```rust
crates/ruuster-config/src/lib.rs

pub struct BrokerConfig {
    pub max_queues: usize,
    pub max_connections: usize,
    pub default_prefetch: u16,
    pub ack_timeout: Duration,
    // ...
}
```

### 3. **Improve Error Types**
Make errors more granular:
```rust
pub enum RuusterError {
    QueueFull { queue_name: String, capacity: u64 },
    QueueNotFound { queue_name: String },
    ExchangeNotFound { exchange_name: String },
    AckTimeout { message_id: Uuid, elapsed: Duration },
    // ...
}
```

---

## 📊 Comparison with RabbitMQ Features

| Feature | RabbitMQ | Ruuster | Priority |
|---------|----------|---------|----------|
| Direct routing | ✅ | ✅ | - |
| Topic routing | ✅ | ✅ | - |
| Fanout routing | ✅ | ✅ | - |
| Header routing | ✅ | ✅ | - |
| Acknowledgments | ✅ | ⚠️ Partial | **HIGH** |
| QoS/Prefetch | ✅ | ❌ | **HIGH** |
| DLX | ✅ | ❌ | **HIGH** |
| Persistence | ✅ | ❌ | **MEDIUM** |
| TTL | ✅ | ❌ | **MEDIUM** |
| Priority queues | ✅ | ❌ | LOW |
| Delayed messages | ✅ (plugin) | ❌ | LOW |
| Management UI | ✅ | ❌ | MEDIUM |
| Clustering | ✅ | ❌ | LOW (later) |
| Auth/RBAC | ✅ | ❌ | MEDIUM |

---

## 🚀 Quick Wins (Low-hanging fruit)

1. **Complete acks_service.rs** - Already scaffolded, just needs impl
2. **Add Prometheus metrics** - Tracing already there, add counters
3. **Create more examples** - Show all routing strategies
4. **Improve logging** - Add structured logging with `tracing`
5. **Add benchmarks** - Use `criterion` for perf tracking

---

**Bottom Line:** Focus on **acknowledgments → QoS → DLX → persistence** in that order. These are table-stakes for any production message broker. The rest can wait.

zabor2432: What should be left in message_service? also Is my architecture of server ok? Elaborate

GitHub Copilot: Excellent question! Let me analyze your current architecture and provide specific recommendations. 

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-server/src/server.rs)

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-server/src/topology_service.rs#1-1), lines 1 to 100

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-server/src/acks_service.rs)

Read [](file:///home/zabor/Projects/ruuster/crates/ruuster-server/src)

## 🏗️ Architecture Analysis & Recommendations

### **Current Architecture: ✅ GOOD Foundation, but needs refinement**

Your current structure is clean and follows good separation of concerns, but it's **too thin** in some areas. Here's what I recommend:

---

## 📋 What Should Stay in message_service.rs

**message_service.rs should be a THIN gRPC adapter layer that:**

```rust
// ✅ KEEP - gRPC trait implementation
#[tonic::async_trait]
impl MessageService for RuusterServer {
    async fn produce(...) -> Result<...> {
        // 1. Parse/validate gRPC request
        // 2. Convert proto -> internal types
        // 3. Delegate to business logic layer
        // 4. Convert result -> proto response
    }

    async fn consume(...) -> Result<...> {
        // Same pattern - thin wrapper
    }
}
```

**✅ SHOULD STAY:**
- gRPC trait implementations
- Proto ↔ Internal type conversions
- Request validation (basic)
- Response formatting

**❌ SHOULD MOVE OUT:**
- Consumer lifecycle management (currently inline in `consume()`)
- Message streaming logic
- QoS/prefetch enforcement
- Any business logic

---

## 🎯 Recommended Architecture Refactoring

### **Problem 1: `RuusterServer` is too simple**

```rust
// ❌ CURRENT - Just a router wrapper
pub struct RuusterServer {
    pub router: Arc<Router>,
}
```

**Issue:** As you add acks, QoS, consumers, etc., you'll need more state. Putting everything in `router` is wrong - router should only handle routing!

### **✅ PROPOSED: Multi-layer Architecture**

```
┌─────────────────────────────────────┐
│   gRPC Services (Thin Adapters)     │
│  - message_service.rs                │
│  - topology_service.rs               │
│  - acks_service.rs                   │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      RuusterServer (Orchestrator)    │
│  - router: Arc<Router>               │
│  - consumer_manager: ConsumerManager │
│  - ack_manager: Arc<AckManager>      │
│  - config: BrokerConfig              │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     Business Logic Layer             │
│  - Router (routing logic)            │
│  - ConsumerManager (consumer state)  │
│  - AckManager (ack tracking)         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│       Core Primitives                │
│  - Queue, Exchange, Message          │
└─────────────────────────────────────┘
```

---

## 📁 Proposed File Structure

```
crates/ruuster-server/src/
├── lib.rs                    # Public API exports
├── main.rs                   # CLI entry point
├── server.rs                 # RuusterServer struct (NEW - see below)
│
├── services/                 # ✅ THIN gRPC adapters
│   ├── mod.rs
│   ├── message_service.rs    # Produce/Consume gRPC impl
│   ├── topology_service.rs   # AddQueue/AddExchange gRPC impl
│   └── acks_service.rs       # Ack/Nack gRPC impl
│
├── domain/                   # ✅ Business logic (NEW)
│   ├── mod.rs
│   ├── consumer_manager.rs   # Consumer lifecycle & state
│   ├── message_handler.rs    # Produce/consume orchestration
│   └── topology_manager.rs   # Exchange/queue lifecycle
│
├── config.rs                 # Broker configuration
└── error.rs                  # Server-specific errors
```

---

## 🔧 Detailed Refactoring Plan

### **1. Create `domain/consumer_manager.rs`** (NEW)

**Why:** Consumer state is scattered. Need centralized management.

```rust
// crates/ruuster-server/src/domain/consumer_manager.rs

use dashmap::DashMap;
use ruuster_core::Queue;
use std::sync::Arc;
use uuid::Uuid;

/// Tracks active consumers and their state
pub struct ConsumerManager {
    /// consumer_id -> ConsumerState
    consumers: DashMap<Uuid, ConsumerState>,
}

struct ConsumerState {
    consumer_id: Uuid,
    queue_name: String,
    queue: Arc<Queue>,
    prefetch_count: u16,
    unacked_messages: Vec<Uuid>, // Message IDs awaiting ack
    last_activity: std::time::Instant,
}

impl ConsumerManager {
    pub fn new() -> Self {
        Self {
            consumers: DashMap::new(),
        }
    }

    /// Register a new consumer
    pub fn register_consumer(
        &self,
        queue: Arc<Queue>,
        prefetch_count: u16,
    ) -> Uuid {
        let consumer_id = Uuid::new_v4();
        let state = ConsumerState {
            consumer_id,
            queue_name: queue.name.clone(),
            queue,
            prefetch_count,
            unacked_messages: Vec::new(),
            last_activity: std::time::Instant::now(),
        };
        self.consumers.insert(consumer_id, state);
        consumer_id
    }

    /// Check if consumer can receive more messages (prefetch limit)
    pub fn can_consume(&self, consumer_id: &Uuid) -> bool {
        self.consumers
            .get(consumer_id)
            .map(|c| c.unacked_messages.len() < c.prefetch_count as usize)
            .unwrap_or(false)
    }

    /// Track message delivery (before ack)
    pub fn track_delivery(&self, consumer_id: &Uuid, message_id: Uuid) {
        if let Some(mut consumer) = self.consumers.get_mut(consumer_id) {
            consumer.unacked_messages.push(message_id);
            consumer.last_activity = std::time::Instant::now();
        }
    }

    /// Remove tracked message (after ack)
    pub fn untrack_message(&self, consumer_id: &Uuid, message_id: &Uuid) {
        if let Some(mut consumer) = self.consumers.get_mut(consumer_id) {
            consumer.unacked_messages.retain(|id| id != message_id);
        }
    }

    /// Cleanup dead consumers (heartbeat timeout)
    pub fn cleanup_stale_consumers(&self, timeout: std::time::Duration) {
        let now = std::time::Instant::now();
        self.consumers.retain(|_, consumer| {
            now.duration_since(consumer.last_activity) < timeout
        });
    }
}
```

---

### **2. Create `domain/message_handler.rs`** (NEW)

**Why:** Produce/consume logic should be separate from gRPC layer.

```rust
// crates/ruuster-server/src/domain/message_handler.rs

use futures::Stream;
use ruuster_internals::Message;
use ruuster_router::Router;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use super::consumer_manager::ConsumerManager;
use crate::error::ServerError;

pub struct MessageHandler {
    router: Arc<Router>,
    consumer_manager: Arc<ConsumerManager>,
}

impl MessageHandler {
    pub fn new(router: Arc<Router>, consumer_manager: Arc<ConsumerManager>) -> Self {
        Self {
            router,
            consumer_manager,
        }
    }

    /// Publish a message to an exchange
    pub fn produce(&self, exchange_name: &str, message: Message) -> Result<(), ServerError> {
        self.router
            .route(exchange_name, message)
            .map_err(ServerError::from)
    }

    /// Start consuming from a queue with QoS support
    pub fn consume(
        &self,
        queue_name: String,
        prefetch_count: u16,
    ) -> Result<MessageStream, ServerError> {
        let queue = self
            .router
            .get_queue(&queue_name)
            .ok_or_else(|| ServerError::QueueNotFound(queue_name.clone()))?;

        let consumer_id = self.consumer_manager.register_consumer(queue, prefetch_count);

        Ok(self.create_message_stream(consumer_id, queue_name))
    }

    fn create_message_stream(
        &self,
        consumer_id: Uuid,
        queue_name: String,
    ) -> MessageStream {
        let router = Arc::clone(&self.router);
        let consumer_mgr = Arc::clone(&self.consumer_manager);

        let stream = async_stream::try_stream! {
            while let Some(queue) = router.get_queue(&queue_name) {
                // ✅ PROPER: Check prefetch limit before dequeuing
                if !consumer_mgr.can_consume(&consumer_id) {
                    // Wait for ack before consuming more
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    continue;
                }

                if let Some(msg) = queue.dequeue()? {
                    let msg_id = msg.uuid;
                    
                    // Track unacked message
                    consumer_mgr.track_delivery(&consumer_id, msg_id);
                    
                    yield msg;
                    continue;
                }

                // Wait for new messages
                queue.notify.notified().await;
            }
        };

        Box::pin(stream)
    }
}

pub type MessageStream = Pin<Box<dyn Stream<Item = Result<Message, ruuster_internals::RuusterError>> + Send>>;
```

---

### **3. Refactor server.rs** (UPDATE)

```rust
// crates/ruuster-server/src/server.rs

use ruuster_acks::AckManager;
use ruuster_router::Router;
use std::sync::Arc;

use crate::domain::consumer_manager::ConsumerManager;
use crate::domain::message_handler::MessageHandler;

/// Central broker server state
pub struct RuusterServer {
    // Routing layer
    pub router: Arc<Router>,
    
    // Consumer management
    pub consumer_manager: Arc<ConsumerManager>,
    
    // Acknowledgment tracking
    pub ack_manager: Arc<AckManager>,
    
    // Message produce/consume orchestration
    pub message_handler: Arc<MessageHandler>,
}

impl RuusterServer {
    pub fn new(router: Arc<Router>) -> Self {
        let consumer_manager = Arc::new(ConsumerManager::new());
        let ack_manager = Arc::new(AckManager::new());
        let message_handler = Arc::new(MessageHandler::new(
            Arc::clone(&router),
            Arc::clone(&consumer_manager),
        ));

        Self {
            router,
            consumer_manager,
            ack_manager,
            message_handler,
        }
    }
}
```

---

### **4. Refactor message_service.rs** (THIN ADAPTER)

```rust
// crates/ruuster-server/src/services/message_service.rs

use futures::Stream;
use ruuster_internals::Message;
use ruuster_protos::v1::message_service_server::MessageService;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest, ProduceResponse};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::server::RuusterServer;

#[tonic::async_trait]
impl MessageService for RuusterServer {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let inner = request.into_inner();
        
        // 1. Validate & convert proto -> internal
        let proto_msg = inner
            .message
            .ok_or_else(|| Status::invalid_argument("Missing message"))?;
        let exchange_name = inner.exchange;

        let msg = Message::try_from(proto_msg)
            .map_err(|e| Status::internal(format!("Conversion error: {}", e)))?;

        // 2. Delegate to business logic layer
        match self.message_handler.produce(&exchange_name, msg) {
            Ok(_) => Ok(Response::new(ProduceResponse {
                success: true,
                error_message: "".into(),
            })),
            Err(e) => Ok(Response::new(ProduceResponse {
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ProtoMsg, Status>> + Send>>;

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;
        
        // TODO: Get prefetch from request (extend proto)
        let prefetch_count = inner.prefetch_count.unwrap_or(1); 

        // Delegate to message handler
        let msg_stream = self
            .message_handler
            .consume(queue_name, prefetch_count)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Convert Message stream -> ProtoMsg stream
        let proto_stream = async_stream::try_stream! {
            for await msg in msg_stream {
                let msg = msg.map_err(|e| Status::internal(e.to_string()))?;
                yield ProtoMsg::from(msg);
            }
        };

        Ok(Response::new(Box::pin(proto_stream)))
    }
}
```

---

## 🎯 Summary: What Goes Where

| Layer | Responsibility | Examples |
|-------|---------------|----------|
| **`services/*_service.rs`** | gRPC adapters (thin!) | Proto conversion, request validation |
| **`domain/message_handler.rs`** | Produce/consume orchestration | QoS enforcement, streaming logic |
| **`domain/consumer_manager.rs`** | Consumer lifecycle | Track prefetch, heartbeats, cleanup |
| **`domain/topology_manager.rs`** | Exchange/queue lifecycle | Validate configs, manage persistence |
| **server.rs** | State container | Hold managers, provide to services |
| **`ruuster-router`** | Routing only | Exchange bindings, strategy execution |
| **`ruuster-core`** | Primitives | Queue, Message (no business logic) |
| **`ruuster-acks`** | Ack tracking | Track unacked, timeouts, requeue |

---

## ✅ Why This Is Better

**Before:**
- ❌ Consumer logic mixed with gRPC in message_service.rs
- ❌ No prefetch/QoS support
- ❌ Hard to test business logic (coupled to gRPC)
- ❌ `RuusterServer` too simple, will become bloated

**After:**
- ✅ gRPC layer is thin and testable
- ✅ Business logic in `domain/` - easily testable without gRPC
- ✅ Clear separation: transport → orchestration → domain → primitives
- ✅ `RuusterServer` coordinates managers, not business logic
- ✅ Easy to add features (just add to domain layer)

---

## 🚀 Migration Path

1. **Week 1:** Create `domain/consumer_manager.rs` + basic tests
2. **Week 2:** Create `domain/message_handler.rs`, move consume logic
3. **Week 3:** Refactor message_service.rs to delegate to handlers
4. **Week 4:** Add prefetch to proto, wire into consumer manager
5. **Week 5:** Implement ack tracking with consumer manager

This architecture will scale to clustering, persistence, and all advanced features!