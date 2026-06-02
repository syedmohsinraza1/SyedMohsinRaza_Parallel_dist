package indras.core;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * INDRAS – Intelligent National Disaster Response & Analytics System
 * Core Producer-Consumer implementation with thread pool management.
 *
 * Parallel and Distributed Computing Lab – Final Term
 */
public class INDRASEventProcessor {

    // ── Thread Pool Configuration ──────────────────────────────────────────
    private static final int INGESTION_CORE   = 32;
    private static final int INGESTION_MAX    = 64;
    private static final int CLASSIFY_CORE    = 16;
    private static final int CLASSIFY_MAX     = 32;
    private static final int DISPATCH_CORE    = 8;
    private static final int DISPATCH_MAX     = 16;
    private static final int QUEUE_CAPACITY   = 10_000;

    // ── Shared Queues (Bounded Blocking Queues) ───────────────────────────
    private final PriorityBlockingQueue<EmergencyEvent> ingestionQueue;
    private final PriorityBlockingQueue<EmergencyEvent> classifyQueue;
    private final LinkedBlockingQueue<EmergencyEvent>   dispatchQueue;

    // ── Thread Pools ──────────────────────────────────────────────────────
    private final ThreadPoolExecutor ingestionPool;
    private final ThreadPoolExecutor classifyPool;
    private final ThreadPoolExecutor dispatchPool;

    // ── Shared Resources with Fine-Grained Locking ────────────────────────
    private final ConcurrentHashMap<String, ResourceUnit> ambulanceRegistry;
    private final ConcurrentHashMap<String, RegionStatus> regionalMap;
    private final ConcurrentLinkedDeque<String>           globalEventLog;
    private final AtomicInteger                           dispatchCounter;

    // ReadWriteLock for Ambulance Registry (high-frequency reads)
    private final ReadWriteLock ambulanceLock = new ReentrantReadWriteLock();
    // StampedLock for Regional Map (optimistic reads)
    private final StampedLock   regionalLock  = new StampedLock();

    // Duplicate event detection
    private final ConcurrentHashMap<String, Boolean> seenEventIds;

    // Lock ordering: ResourceRegistry=1, RegionRegistry=2 (deadlock prevention)
    private static final int RESOURCE_LOCK_ORDER = 1;
    private static final int   REGION_LOCK_ORDER = 2;

    public INDRASEventProcessor() {
        this.ingestionQueue = new PriorityBlockingQueue<>(QUEUE_CAPACITY, EmergencyEvent.BY_SEVERITY);
        this.classifyQueue  = new PriorityBlockingQueue<>(5_000,          EmergencyEvent.BY_SEVERITY);
        this.dispatchQueue  = new LinkedBlockingQueue<>(2_000);

        this.ambulanceRegistry = new ConcurrentHashMap<>();
        this.regionalMap       = new ConcurrentHashMap<>();
        this.globalEventLog    = new ConcurrentLinkedDeque<>();
        this.dispatchCounter   = new AtomicInteger(0);
        this.seenEventIds      = new ConcurrentHashMap<>();

        // Ingestion pool: I/O-bound → N_cpu × (1 + Wait/Compute) = 8×7 ≈ 64
        this.ingestionPool = new ThreadPoolExecutor(
            INGESTION_CORE, INGESTION_MAX, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(QUEUE_CAPACITY),
            new NamedThreadFactory("ingestion"),
            new ThreadPoolExecutor.CallerRunsPolicy()   // Backpressure, not drop
        );

        // Classification pool: CPU-bound → N_cpu × 1.25 ≈ 16-32
        this.classifyPool = new ThreadPoolExecutor(
            CLASSIFY_CORE, CLASSIFY_MAX, 60L, TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(5_000),
            new NamedThreadFactory("classify"),
            new ThreadPoolExecutor.AbortPolicy()
        );

        // Dispatch pool: Mixed I/O + CPU
        this.dispatchPool = new ThreadPoolExecutor(
            DISPATCH_CORE, DISPATCH_MAX, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2_000),
            new NamedThreadFactory("dispatch"),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    // ── PRODUCER: Submit event from any data source ───────────────────────
    public void submitEvent(EmergencyEvent event) {
        // Duplicate detection (lock-free via ConcurrentHashMap)
        if (seenEventIds.putIfAbsent(event.getId(), Boolean.TRUE) != null) {
            return; // duplicate – silently drop
        }
        // Submit to ingestion pool (non-blocking submission, backpressure via CallerRuns)
        ingestionPool.submit(() -> processIngest(event));
    }

    private void processIngest(EmergencyEvent event) {
        // Normalize & enqueue for classification
        event.setReceivedTimestamp(System.currentTimeMillis());
        classifyPool.submit(() -> classify(event));
    }

    // ── CONSUMER: Classification ──────────────────────────────────────────
    private void classify(EmergencyEvent event) {
        // Assign event type and severity score (1–10)
        EventClassifier.classify(event);
        if (event.getSeverity() >= 8) {
            // Escalate: push to global cross-region queue
            globalEventLog.addFirst(event.toString());
        }
        dispatchPool.submit(() -> dispatch(event));
    }

    // ── CONSUMER: Dispatch ────────────────────────────────────────────────
    private void dispatch(EmergencyEvent event) {
        // Deadlock-free resource + region lock acquisition (LOCK ORDERING ENFORCED)
        // Always acquire RESOURCE lock (order=1) before REGION lock (order=2)
        Lock resourceLock = getResourceLock(event.getResourceType()); // order=1
        Lock regionLock   = getRegionLock(event.getRegion());         // order=2

        boolean gotResource = false, gotRegion = false;
        try {
            gotResource = resourceLock.tryLock(500, TimeUnit.MILLISECONDS);
            if (gotResource) {
                gotRegion = regionLock.tryLock(500, TimeUnit.MILLISECONDS);
            }
            if (gotResource && gotRegion) {
                // Critical section: allocate resource
                ResourceUnit unit = findAvailableUnit(event);
                if (unit != null) {
                    unit.assignTo(event);
                    dispatchCounter.incrementAndGet();
                    updateRegionalMap(event.getRegion());
                }
            } else {
                // Backoff + retry to prevent starvation
                Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
                dispatchPool.submit(() -> dispatch(event));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (gotRegion)   regionLock.unlock();
            if (gotResource) resourceLock.unlock();
        }
    }

    // ── Ambulance Registry access with ReadWriteLock ──────────────────────
    public ResourceUnit getAmbulanceUnit(String id) {
        ambulanceLock.readLock().lock();  // Multiple readers allowed
        try {
            return ambulanceRegistry.get(id);
        } finally {
            ambulanceLock.readLock().unlock();
        }
    }

    public void updateAmbulanceStatus(String id, ResourceUnit unit) {
        ambulanceLock.writeLock().lock(); // Exclusive write
        try {
            ambulanceRegistry.put(id, unit);
        } finally {
            ambulanceLock.writeLock().unlock();
        }
    }

    // ── Regional Map access with StampedLock (optimistic) ────────────────
    public RegionStatus getRegionStatus(String region) {
        long stamp = regionalLock.tryOptimisticRead();
        RegionStatus status = regionalMap.get(region);
        if (!regionalLock.validate(stamp)) {
            // Concurrent write happened – fallback to pessimistic read
            stamp = regionalLock.readLock();
            try { status = regionalMap.get(region); }
            finally { regionalLock.unlockRead(stamp); }
        }
        return status;
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────
    public void shutdown() throws InterruptedException {
        ingestionPool.shutdown();
        classifyPool.shutdown();
        dispatchPool.shutdown();
        ingestionPool.awaitTermination(30, TimeUnit.SECONDS);
        classifyPool.awaitTermination(30, TimeUnit.SECONDS);
        dispatchPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    // ── Thread factory for named threads (easier debugging) ──────────────
    static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger count = new AtomicInteger(0);
        NamedThreadFactory(String prefix) { this.prefix = prefix; }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-thread-" + count.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }

    // Stub methods (implementations omitted for brevity)
    private Lock getResourceLock(String resourceType) { return new ReentrantLock(); }
    private Lock getRegionLock(String region)         { return new ReentrantLock(); }
    private ResourceUnit findAvailableUnit(EmergencyEvent e) { return null; }
    private void updateRegionalMap(String region) { regionalMap.put(region, new RegionStatus()); }

    // Stub classes
    static class EmergencyEvent implements Comparable<EmergencyEvent> {
        static final java.util.Comparator<EmergencyEvent> BY_SEVERITY =
            java.util.Comparator.comparingInt(EmergencyEvent::getSeverity).reversed();
        private String id, region, resourceType;
        private int severity;
        private long receivedTimestamp;
        public String getId()           { return id; }
        public String getRegion()       { return region; }
        public String getResourceType() { return resourceType; }
        public int getSeverity()        { return severity; }
        public void setReceivedTimestamp(long ts) { this.receivedTimestamp = ts; }
        @Override public int compareTo(EmergencyEvent o) { return o.severity - this.severity; }
    }
    static class ResourceUnit {
        void assignTo(EmergencyEvent e) {}
    }
    static class RegionStatus {}
    static class EventClassifier {
        static void classify(EmergencyEvent e) {}
    }
}
