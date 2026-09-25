@file:androidx.annotation.OptIn(markerClass = [androidx.media3.common.util.UnstableApi::class])

package blbl.cat3399.feature.player.engine

import android.net.Uri
import android.os.SystemClock
import androidx.media3.common.C
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.TransferListener
import blbl.cat3399.core.log.AppLog
import blbl.cat3399.feature.player.CdnFailoverState
import blbl.cat3399.feature.player.DebugStreamKind
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.Closeable
import java.io.IOException
import java.io.InterruptedIOException
import java.util.ArrayDeque
import java.util.Collections
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private fun rangeNowMs(): Long = SystemClock.elapsedRealtime()

// SocketTimeoutException also extends InterruptedIOException; it MUST still permit fallback.
internal class RangeCanceledException(message: String) : InterruptedIOException(message)

/** Bandwidth/health are keyed by host AND media path; audio/init cannot rank a video route. */
internal class CdnSpeedTracker {
    private val routes = LinkedHashMap<String, RangeRouteMeasurement>(32, 0.75f, true)
    private val activeHosts = HashMap<String, Int>()
    private val unsupported = LinkedHashMap<String, Long>()
    private var probeCursor = 0
    private fun host(uri: Uri) = uri.host.orEmpty().lowercase(Locale.US)
    private fun resource(uri: Uri) = host(uri) + uri.encodedPath.orEmpty()
    private fun entry(uri: Uri): RangeRouteMeasurement = routes.getOrPut(resource(uri)) {
        if (routes.size >= 512) routes.remove(routes.keys.first())
        RangeRouteMeasurement()
    }

    @Synchronized
    fun supportsRange(uri: Uri): Boolean = (unsupported[resource(uri)] ?: 0L) <= rangeNowMs()

    @Synchronized
    fun markUnsupported(uri: Uri) {
        unsupported[resource(uri)] = rangeNowMs() + 300_000L
        if (unsupported.size > 128) unsupported.remove(unsupported.keys.first())
    }

    @Synchronized
    fun ordered(candidates: List<Uri>, preferred: Int, mode: String = "auto", balance: Boolean = false, explore: Boolean = false): List<Uri> {
        val now = rangeNowMs()
        val rotated = candidates.indices.map { candidates[(preferred + it) % candidates.size] }
        fun region(uri: Uri): Int = BilibiliCdnRoutes.regionRank(host(uri), mode)
        val ranked = rotated.filter(::supportsRange).sortedWith(
            compareBy<Uri> { entry(it).blockedUntil > now }
                .thenBy { region(it) }
                .thenByDescending {
                    entry(it).speed(now) / if (balance) ((activeHosts[host(it)] ?: 0) + 1).toDouble() else 1.0
                },
        )
        // Occasional ahead-of-head probes, including low-buffer recovery. Do not wait for an
        // 8-second buffer that a bad initial route might never be able to build.
        if (explore && probeCursor++ % 4 == 0) {
            val bestRegion = ranked.firstOrNull()?.let(::region)
            val probe = ranked.firstOrNull {
                entry(it).speed(now) == 0.0 && entry(it).blockedUntil <= now &&
                    (activeHosts[host(it)] ?: 0) == 0 && region(it) == bestRegion
            }
            if (probe != null) return listOf(probe) + ranked.filter { it != probe }
        }
        return ranked
    }

    @Synchronized fun started(uri: Uri) { activeHosts[host(uri)] = (activeHosts[host(uri)] ?: 0) + 1 }
    @Synchronized fun finished(uri: Uri) { activeHosts[host(uri)] = ((activeHosts[host(uri)] ?: 0) - 1).coerceAtLeast(0) }
    @Synchronized fun speed(uri: Uri): Double = entry(uri).speed(rangeNowMs())
    @Synchronized fun firstByteMs(uri: Uri): Long = entry(uri).firstByteMs
    @Synchronized fun available(uri: Uri): Boolean = supportsRange(uri) && entry(uri).blockedUntil <= rangeNowMs()

    @Synchronized
    fun recordTransfer(uri: Uri, bytes: Long, elapsedMs: Long, ttfbMs: Long, complete: Boolean, media: Boolean) {
        entry(uri).record(bytes, elapsedMs, ttfbMs, complete, media, rangeNowMs())
    }

    @Synchronized fun recordFailure(uri: Uri) { entry(uri).fail(rangeNowMs()) }
    @Synchronized fun recordSlow(uri: Uri) { entry(uri).slow(rangeNowMs()) }
}

/** Shared per player: bounded memory, priority normal queue and a reserved tail-rescue lane. */
internal class ParallelRangeScheduler(val options: RangeDownloadOptions = RangeDownloadOptions()) : Closeable {
    private class Job(val priority: Int, val sequence: Long, action: () -> Unit) :
        FutureTask<Unit>(Callable { action(); Unit }), Comparable<Job> {
        override fun compareTo(other: Job): Int =
            priority.compareTo(other.priority).takeIf { it != 0 } ?: sequence.compareTo(other.sequence)
    }
    private val sequence = AtomicLong()
    private val normalLimit = options.connectionLimit - if (options.rescueEnabled) 1 else 0
    private val controller = RangeConcurrencyController(normalLimit)
    private fun pool(count: Int, name: String): ThreadPoolExecutor =
        ThreadPoolExecutor(
            count, count, 30L, TimeUnit.SECONDS,
            PriorityBlockingQueue<Runnable>(11, Comparator { a, b -> (a as Job).compareTo(b as Job) }),
            ThreadFactory { r -> Thread(r, name).apply { isDaemon = true } },
        ).apply { allowCoreThreadTimeOut(true) }
    private val primary = pool(if (options.automatic) controller.limit else normalLimit, "blbl-range")
    private val rescue = pool(1, "blbl-range-rescue")
    private val sessions = Collections.newSetFromMap(ConcurrentHashMap<ParallelRangeSession, Boolean>())
    private var allocatedBytes = 0
    private val networkBytes = AtomicLong()
    private val uniqueBytes = AtomicLong()
    private var lastStatsAt = rangeNowMs()
    private var lastNetworkBytes = 0L
    private var lastUniqueBytes = 0L
    @Volatile private var closed = false
    @Volatile var bufferedMs: Long = 0L
        private set
    @Volatile private var requiredBytesPerSecond = 0.0
    val primaryCapacity: Int get() = primary.corePoolSize
    val activeRequests: Int get() = primary.activeCount + rescue.activeCount

    init {
        if (options.debug) AppLog.i("RangeAccel", "config v=2 enabled=${options.enabled} auto=${options.automatic}" +
            " limit=${options.connectionLimit} cdnMode=${options.cdnMode} rescue=${options.rescueEnabled} windowBytes=${options.memoryBytes}")
    }

    @Synchronized fun updatePlayback(bufferMs: Long, bitrate: Long? = null) {
        bufferedMs = bufferMs.coerceAtLeast(0L)
        if (bitrate != null) requiredBytesPerSecond = bitrate.coerceAtLeast(0L) / 8.0
        if (closed) return
        val now = rangeNowMs()
        if (options.debug && now - lastStatsAt >= 4_000L) {
            val elapsed = now - lastStatsAt
            val physical = networkBytes.get()
            val unique = uniqueBytes.get()
            if (activeRequests > 0 || unique != lastUniqueBytes) AppLog.d("RangeAccel",
                "throughput networkBps=${(physical - lastNetworkBytes) * 1000L / elapsed}" +
                    " usefulBps=${(unique - lastUniqueBytes) * 1000L / elapsed} requiredBps=${requiredBytesPerSecond.toLong()}" +
                    " bufferMs=$bufferedMs slots=$primaryCapacity active=$activeRequests reservedBytes=$allocatedBytes")
            lastStatsAt = now
            lastNetworkBytes = physical
            lastUniqueBytes = unique
        }
        if (!options.automatic) return
        // Video deliberately leaves a normal slot for audio; that slot need not be busy to trial.
        val target = controller.update(now, bufferedMs,
            primary.activeCount >= (primary.corePoolSize - 1).coerceAtLeast(1), requiredBytesPerSecond)
        if (options.debug && target != primary.corePoolSize) {
            AppLog.d("RangeAccel", "normal slots=" + primary.corePoolSize + "->" + target + " bufferMs=" + bufferedMs)
        }
        if (target > primary.corePoolSize) {
            primary.maximumPoolSize = target
            primary.corePoolSize = target
        } else if (target < primary.corePoolSize) {
            primary.corePoolSize = target
            primary.maximumPoolSize = target
        }
    }
    fun usefulBytes(count: Int) {
        uniqueBytes.addAndGet(count.toLong())
        controller.record(count)
        if (options.automatic || options.debug) updatePlayback(bufferedMs)
    }
    fun receivedBytes(count: Int) { networkBytes.addAndGet(count.toLong()) }
    @Synchronized fun reserve(bytes: Int, critical: Boolean): Boolean {
        val limit = options.memoryBytes - if (critical) 0 else 2 * ParallelRangeConfig.HEAD_BYTES
        if (allocatedBytes + bytes > limit) return false
        allocatedBytes += bytes
        return true
    }
    @Synchronized fun release(bytes: Int) { allocatedBytes -= bytes }
    @Synchronized fun reservedBytes(): Int = allocatedBytes
    fun register(session: ParallelRangeSession) {
        if (closed) throw RangeCanceledException("Scheduler closed")
        sessions.add(session)
        if (closed) {
            session.close()
            throw RangeCanceledException("Scheduler closed")
        }
    }
    fun unregister(session: ParallelRangeSession) { sessions.remove(session) }
    fun cancelAll() { sessions.toList().forEach { it.close() } }
    fun submit(priority: Int, isRescue: Boolean, action: () -> Unit): Future<*> {
        val job = Job(priority, sequence.getAndIncrement(), action)
        (if (isRescue) rescue else primary).execute(job)
        return job
    }
    fun cancel(job: Future<*>) {
        job.cancel(true)
        if (job is Runnable) {
            primary.remove(job)
            rescue.remove(job)
        }
    }
    override fun close() {
        closed = true
        cancelAll()
        primary.shutdownNow()
        rescue.shutdownNow()
    }
}

/**
 * The reader sees a contiguous prefix immediately, not a Future<ByteArray>.
 * Only a small rolling window exists. A hedge requests the missing tail and publishes into
 * the SAME prefix buffer; overlapping bytes are ignored, so no duplicated bytes reach the decoder.
 */
internal class ParallelRangeSession(
    private val client: OkHttpClient,
    candidates: List<Uri>,
    private val state: CdnFailoverState,
    private val scheduler: ParallelRangeScheduler,
    private val speedTracker: CdnSpeedTracker,
    private val start: Long,
    private val length: Long,
    private val requestHeaders: Map<String, String> = emptyMap(),
    private val onHost: ((String) -> Unit)? = null,
    private val onNetworkBytes: ((Long) -> Unit)? = null,
    private val originalUri: Uri? = null,
) : Closeable {
    private val candidates = candidates.distinct()
    private val total = AtomicLong(-1L)
    private val lock = Any()
    private val pending = ArrayDeque<Piece>()
    private var cursor = start
    private var first = true
    private val metadata = start < ParallelRangeConfig.HEAD_BYTES && length in 1L..ParallelRangeConfig.HEAD_BYTES.toLong()
    private val phase = if (metadata) "meta" else "media"
    private val resourceId = Integer.toHexString(this.candidates.first().encodedPath.orEmpty().hashCode())
    @Volatile private var headComplete = false
    @Volatile private var end = if (length == C.LENGTH_UNSET.toLong()) Long.MAX_VALUE - 1L else start + length - 1L
    @Volatile private var closed = false
    val totalLength: Long get() = total.get()
    val resolvedLength: Long get() = end - start + 1L

    init {
        require(start >= 0L && (length == -1L || (length > 0L && length <= Long.MAX_VALUE - start)))
        require(this.candidates.isNotEmpty())
        scheduler.register(this)
    }

    private fun ordered(balance: Boolean = false, explore: Boolean = false) =
        speedTracker.ordered(candidates, state.getPreferredIndex(), scheduler.options.cdnMode, balance,
            explore = explore)

    fun open(): Long {
        headComplete = !metadata &&
            ordered().firstOrNull()?.let { speedTracker.speed(it) > 0.0 } == true
        refill()
        val head = synchronized(lock) { pending.peekFirst() } ?: throw IOException("No range head")
        head.awaitHeaders()
        if (scheduler.options.debug) {
            AppLog.i("RangeAccel", "stream kind=" + state.kind + " phase=$phase resource=$resourceId position=" + start +
                " length=" + resolvedLength + " limit=" + scheduler.options.connectionLimit +
                " active=" + scheduler.activeRequests + " reservedBytes=" + scheduler.reservedBytes() +
                " routes=" + ordered().map { it.host }.distinct().joinToString(","))
        }
        return resolvedLength
    }

    private fun refill() = synchronized(lock) {
        checkOpen()
        val window =
            if (metadata || !headComplete || state.kind == DebugStreamKind.AUDIO) 1
            else (scheduler.primaryCapacity - 1).coerceIn(1, 6)
        while (pending.size < window && cursor <= end) {
            val ranked = ordered(balance = pending.isNotEmpty(), explore = !metadata && pending.isNotEmpty())
            // Metadata may use the original API address while racing a pool node. Its winner
            // never supplies a media bandwidth sample or suppresses the subsequent media probe.
            val primary = originalUri?.takeIf { metadata && first && it in ranked && speedTracker.available(it) }
                ?: ranked.firstOrNull() ?: throw IOException("Range unavailable")
            val range = ParallelRangePlanner.next(cursor, end, first && !headComplete, speedTracker.speed(primary))
            val critical = pending.isEmpty()
            if (!scheduler.reserve(range.length.toInt(), critical)) {
                if (critical) throw IOException("Range memory budget exhausted")
                break
            }
            val piece = Piece(range, first)
            val probe = first && speedTracker.speed(primary) == 0.0 &&
                scheduler.options.rescueEnabled && candidates.size > 1
            first = false
            pending.addLast(piece)
            cursor = range.end + 1L
            piece.launch(isRescue = false, primary = primary)
            // One bounded startup race, using the reserved lane. Unlike distributing the
            // initial segment over unknown nodes, either response can supply the same prefix.
            if (probe) piece.launch(isRescue = true, probe = true)
        }
    }

    fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        while (true) {
            refill()
            val piece = synchronized(lock) { pending.peekFirst() } ?: return C.RESULT_END_OF_INPUT
            val count = piece.read(buffer, offset, length)
            if (count >= 0) return count
            synchronized(lock) {
                if (pending.peekFirst() === piece) pending.removeFirst()
            }
            piece.dispose()
        }
    }

    private fun checkOpen() {
        if (closed || Thread.currentThread().isInterrupted) throw RangeCanceledException("Range canceled")
    }

    private fun acceptMetadata(content: ParallelContentRange) {
        total.compareAndSet(-1L, content.total)
        if (total.get() != content.total) throw IOException("CDN resource length changed")
        end = minOf(end, content.total - 1L)
    }

    override fun close() {
        val pieces = synchronized(lock) {
            if (closed) return
            closed = true
            pending.toList().also { pending.clear() }
        }
        pieces.forEach { it.dispose() }
        scheduler.unregister(this)
    }

    private inner class Piece(val range: ParallelByteRange, private val isHead: Boolean) {
        private val monitor = Object()
        private var bytes: ByteArray? = ByteArray(range.length.toInt())
        private var size = range.length.toInt()
        private var filled = 0
        private var consumed = 0
        private var headers = false
        private var disposed = false
        private var running = 0
        private var attempts = 0
        private var rescued = false
        private val tried = HashSet<Uri>()
        private var error: IOException? = null
        private val calls = ConcurrentHashMap<Call, Uri>()
        private val jobs = CopyOnWriteArrayList<Future<*>>()
        private val createdAt = rangeNowMs()
        private var lastProgress = createdAt
        private var criticalSince = 0L

        fun launch(isRescue: Boolean, primary: Uri? = null, probe: Boolean = false) {
            val uri = synchronized(monitor) {
                if (disposed || closed || (headers && filled >= size) || attempts >= 3) return
                val ranked = ordered(balance = !isHead)
                val chosen = primary?.takeIf { it in ranked }
                    ?: ranked.firstOrNull { it !in tried && tried.none { previous -> previous.host == it.host } }
                    ?: ranked.firstOrNull { it !in tried }
                    ?: ranked.firstOrNull { it !in calls.values }
                    ?: return
                if (isRescue && (rescued || chosen in calls.values)) return
                if (isRescue) rescued = true
                attempts++
                running++
                tried.add(chosen)
                chosen
            }
            try {
                if (isRescue && !probe) calls.values.filter { it != uri }.forEach(speedTracker::recordSlow)
                val job = scheduler.submit(
                    priority = if (isRescue || isHead) 0 else if (state.kind == DebugStreamKind.AUDIO) 1 else 10,
                    isRescue = isRescue,
                ) { download(uri) }
                jobs.add(job)
                if (synchronized(monitor) { disposed }) scheduler.cancel(job)
                if (isRescue && scheduler.options.debug) AppLog.d("RangeAccel",
                    (if (probe) "startup probe host=" else "rescue tail host=") + uri.host +
                        " kind=${state.kind} phase=$phase resource=$resourceId start=${range.start} bufferMs=${scheduler.bufferedMs}")
            } catch (e: RuntimeException) {
                synchronized(monitor) {
                    running--
                    error = IOException("Range scheduling failed", e)
                    monitor.notifyAll()
                }
            }
        }

        private fun rescueIfNeeded() {
            if (!scheduler.options.rescueEnabled || candidates.size < 2) return
            val due = synchronized(monitor) {
                val elapsed = rangeNowMs() - createdAt
                val remainingMs = if (filled > 0) (size - filled) * elapsed / filled else Long.MAX_VALUE
                val latency = calls.values.firstOrNull()?.let(speedTracker::firstByteMs) ?: 0L
                val delay = if (scheduler.bufferedMs < 3_000L) (latency * 2L).coerceIn(350L, 900L)
                    else ParallelRangeConfig.STALL_MS
                val budget = (scheduler.bufferedMs - 500L).coerceAtLeast(700L)
                !rescued && !disposed && elapsed >= delay &&
                    (rangeNowMs() - lastProgress >= ParallelRangeConfig.STALL_MS ||
                        remainingMs > budget)
            }
            if (due) launch(isRescue = true)
        }

        private fun waitForData(headersOnly: Boolean) {
            while (true) {
                checkOpen()
                synchronized(monitor) {
                    if (disposed) throw RangeCanceledException("Range closed")
                    if (if (headersOnly) headers else filled > consumed || (headers && consumed >= size)) return
                    if (running == 0) throw error ?: IOException("No usable range route")
                    if (criticalSince == 0L) criticalSince = rangeNowMs()
                    // A slow trickle must not reset the deadline on every DataSource.read().
                    if (rangeNowMs() - criticalSince >= ParallelRangeConfig.CRITICAL_WAIT_MS) throw IOException("Range deadline exceeded")
                }
                rescueIfNeeded()
                synchronized(monitor) {
                    if (disposed) throw RangeCanceledException("Range closed")
                    if (if (headersOnly) headers else filled > consumed || (headers && consumed >= size)) return
                    try {
                        monitor.wait(100L)
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw RangeCanceledException("Range wait canceled")
                    }
                }
            }
        }

        fun awaitHeaders() { waitForData(headersOnly = true) }
        fun read(target: ByteArray, offset: Int, length: Int): Int {
            waitForData(headersOnly = false)
            return synchronized(monitor) {
                if (consumed >= size) return@synchronized C.RESULT_END_OF_INPUT
                val count = minOf(length, filled - consumed)
                val source = bytes ?: throw RangeCanceledException("Range released")
                source.copyInto(target, offset, consumed, consumed + count)
                consumed += count
                count
            }
        }

        /** Returns only NEW useful bytes, never duplicate hedge traffic. */
        private fun publish(position: Int, source: ByteArray, count: Int): Int = synchronized(monitor) {
            if (disposed || closed) throw RangeCanceledException("Range canceled")
            if (position > filled) throw IOException("Non-contiguous range response")
            val skip = (filled - position).coerceAtMost(count)
            val added = minOf(count - skip, size - filled)
            if (added > 0) {
                source.copyInto(checkNotNull(bytes), filled, skip, skip + added)
                filled += added
                lastProgress = rangeNowMs()
                if (filled == size && isHead) headComplete = true
                monitor.notifyAll()
            }
            added
        }

        private fun download(uri: Uri) {
            var call: Call? = null
            var useful = 0L
            var received = 0L
            val startedAt = rangeNowMs()
            var ttfbMs = -1L
            var responseCode = 0
            var attemptComplete = false
            var failure: String? = null
            speedTracker.started(uri)
            try {
                checkOpen()
                val offset = synchronized(monitor) {
                    if (disposed || (headers && filled >= size)) return
                    filled
                }
                val requestStart = range.start + offset
                val requestEnd = minOf(range.end, end)
                val builder = Request.Builder().url(uri.toString())
                requestHeaders.forEach { (name, value) ->
                    if (!name.equals("Range", true) && !name.equals("Accept-Encoding", true)) builder.header(name, value)
                }
                val request = builder.header("Range", "bytes=" + requestStart + "-" + requestEnd)
                    .header("Accept-Encoding", "identity").get().build()
                val activeCall = client.newCall(request)
                call = activeCall
                calls[activeCall] = uri
                if (synchronized(monitor) { disposed || (headers && filled >= size) } || closed) {
                    activeCall.cancel()
                    return
                }
                activeCall.timeout().timeout(ParallelRangeConfig.CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                activeCall.execute().use { response ->
                    responseCode = response.code
                    val content = parseParallelContentRange(response.header("Content-Range"))
                    if (response.code != 206 || content == null) {
                        if (response.code == 200 || response.code == 206 || response.code == 416) speedTracker.markUnsupported(uri)
                        throw IOException("Invalid range status=" + response.code)
                    }
                    if (content.start != requestStart || content.end != minOf(requestEnd, content.total - 1L)) {
                        speedTracker.markUnsupported(uri)
                        throw IOException("Invalid Content-Range")
                    }
                    val body = response.body ?: throw IOException("Empty range body")
                    if (body.contentLength() >= 0L && body.contentLength() != content.length) throw IOException("Invalid range length")
                    acceptMetadata(content)
                    synchronized(monitor) {
                        size = (minOf(range.end, end) - range.start + 1L).toInt()
                        headers = true
                        monitor.notifyAll()
                    }
                    onHost?.invoke(uri.host.orEmpty())
                    body.byteStream().use { input ->
                        val scratch = ByteArray(16 * 1024)
                        var position = offset
                        val expected = (content.end - range.start + 1L).toInt()
                        while (position < expected) {
                            checkOpen()
                            val count = input.read(scratch, 0, minOf(scratch.size, expected - position))
                            if (count < 0) throw IOException("Truncated range body")
                            if (count == 0) continue
                            if (ttfbMs < 0L) ttfbMs = rangeNowMs() - startedAt
                            received += count
                            scheduler.receivedBytes(count)
                            onNetworkBytes?.invoke(count.toLong())
                            val added = publish(position, scratch, count)
                            useful += added
                            scheduler.usefulBytes(added)
                            position += count
                            if (synchronized(monitor) { filled >= size }) break
                        }
                        attemptComplete = position >= expected
                    }
                }
            } catch (e: IOException) {
                failure = e.javaClass.simpleName
                synchronized(monitor) { error = e }
                if (!closed && synchronized(monitor) { !disposed && filled < size }) {
                    speedTracker.recordFailure(uri)
                }
            } catch (e: RuntimeException) {
                failure = e.javaClass.simpleName
                synchronized(monitor) { error = IOException("Range request failed", e) }
            } finally {
                val completed = synchronized(monitor) { headers && filled >= size }
                val elapsed = rangeNowMs() - startedAt
                // Completion belongs to this HTTP attempt, not to the shared piece. A losing
                // or cancelled request must not masquerade as a successful CDN response.
                if (received > 0L) speedTracker.recordTransfer(uri, received, elapsed, ttfbMs, attemptComplete, !metadata)
                if (scheduler.options.debug) AppLog.d("RangeAccel",
                    "request kind=${state.kind} phase=$phase resource=$resourceId host=${uri.host}" +
                        " start=${range.start} status=$responseCode ttfbMs=$ttfbMs elapsedMs=$elapsed" +
                        " received=$received useful=$useful complete=$attemptComplete error=${failure ?: "none"}")
                if (completed) {
                    val best = ordered().firstOrNull()
                    val index = candidates.indexOf(best)
                    if (index >= 0) state.prefer(state.candidates.indexOf(candidates[index]).coerceAtLeast(0))
                    calls.keys.filter { it !== call }.forEach { it.cancel() }
                }
                call?.let { calls.remove(it) }
                speedTracker.finished(uri)
                // Schedule a tail retry before advertising that all attempts have ended.
                synchronized(monitor) {
                    // Claim/queue the retry atomically with finishing this attempt. Otherwise
                    // two failing racers can both observe running == 2 and neither retries.
                    // launch only queues work; it performs no network IO under this monitor.
                    if (!closed && !disposed && !completed && running == 1 && attempts < 3) {
                        launch(isRescue = false)
                    }
                    running--
                    monitor.notifyAll()
                }
            }
        }

        fun dispose() {
            synchronized(monitor) {
                if (disposed) return
                disposed = true
                bytes = null
                monitor.notifyAll()
            }
            calls.keys.forEach { it.cancel() }
            jobs.forEach(scheduler::cancel)
            scheduler.release(range.length.toInt())
        }
    }
}

internal class ParallelRangeDataSourceFactory(
    private val fallbackFactory: DataSource.Factory,
    private val client: OkHttpClient,
    private val candidates: List<Uri>,
    private val state: CdnFailoverState,
    private val scheduler: ParallelRangeScheduler,
    private val speedTracker: CdnSpeedTracker,
    private val onTransferHost: ((String) -> Unit)? = null,
    private val onNetworkBytes: ((Long) -> Unit)? = null,
) : DataSource.Factory {
    override fun createDataSource(): DataSource =
        ParallelRangeDataSource(fallbackFactory, client, candidates, state, scheduler, speedTracker, onTransferHost, onNetworkBytes)
}

private class ParallelRangeDataSource(
    private val fallbackFactory: DataSource.Factory,
    private val client: OkHttpClient,
    private val candidates: List<Uri>,
    private val state: CdnFailoverState,
    private val scheduler: ParallelRangeScheduler,
    private val speedTracker: CdnSpeedTracker,
    private val onTransferHost: ((String) -> Unit)?,
    private val onNetworkBytes: ((Long) -> Unit)?,
) : DataSource {
    private val listeners = CopyOnWriteArrayList<TransferListener>()
    private val listenerLock = Any()
    private var delegate: DataSource? = null
    private var session: ParallelRangeSession? = null
    private var spec: DataSpec? = null
    private var delivered = 0L
    private var resolvedLength = C.LENGTH_UNSET.toLong()
    private var transferStarted = false

    override fun addTransferListener(transferListener: TransferListener) {
        listeners.addIfAbsent(transferListener)
        delegate?.addTransferListener(transferListener)
    }
    override fun open(dataSpec: DataSpec): Long {
        close()
        spec = dataSpec
        // Only registered media URLs: never replace HLS child/playlist URLs with a parent CDN URL.
        val media = candidates.any { it == dataSpec.uri }
        val path = dataSpec.uri.path.orEmpty().lowercase(Locale.US)
        if (!scheduler.options.enabled || !media ||
            path.endsWith(".m3u8") || path.endsWith(".mpd") ||
            !(dataSpec.uri.scheme.equals("https", true) || dataSpec.uri.scheme.equals("http", true)) ||
            dataSpec.httpMethod != DataSpec.HTTP_METHOD_GET || dataSpec.httpBody != null ||
            dataSpec.length == 0L || candidates.none(speedTracker::supportsRange)
        ) return openFallback(dataSpec)
        synchronized(listenerLock) {
            listeners.forEach { it.onTransferInitializing(this, dataSpec, true) }
            listeners.forEach { it.onTransferStart(this, dataSpec, true) }
            transferStarted = true
        }
        val range = ParallelRangeSession(
            client, candidates, state, scheduler, speedTracker, dataSpec.position, dataSpec.length,
            requestHeaders = dataSpec.httpRequestHeaders, onHost = onTransferHost,
            originalUri = dataSpec.uri,
            onNetworkBytes = { count ->
                synchronized(listenerLock) {
                    if (transferStarted && spec === dataSpec) {
                        onNetworkBytes?.invoke(count)
                        listeners.forEach { it.onBytesTransferred(this, dataSpec, true, count.toInt()) }
                    }
                }
            },
        )
        session = range
        return try {
            range.open().also { resolvedLength = it }
        } catch (e: IOException) {
            range.close()
            session = null
            endTransfer()
            if (Thread.currentThread().isInterrupted || e is RangeCanceledException) throw e
            AppLog.w("RangeAccel", "range unavailable; using original stream: " + e.message)
            openFallback(dataSpec)
        }
    }
    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        delegate?.let { return it.read(buffer, offset, length) }
        if (length == 0) return 0
        if (resolvedLength >= 0L && delivered >= resolvedLength) return C.RESULT_END_OF_INPUT
        return try {
            val count = checkNotNull(session).read(buffer, offset, length)
            if (count > 0) delivered += count
            count
        } catch (e: IOException) {
            session?.close()
            session = null
            endTransfer()
            if (Thread.currentThread().isInterrupted || e is RangeCanceledException) throw e
            val original = checkNotNull(spec)
            val remaining = if (resolvedLength >= 0L) resolvedLength - delivered else C.LENGTH_UNSET.toLong()
            if (remaining == 0L) return C.RESULT_END_OF_INPUT
            val resume = original.buildUpon().setPosition(original.position + delivered).setLength(remaining).build()
            AppLog.w("RangeAccel", "resume original stream at delivered=" + delivered + ": " + e.message)
            openFallback(resume)
            checkNotNull(delegate).read(buffer, offset, length)
        }
    }
    private fun openFallback(dataSpec: DataSpec): Long {
        val next = fallbackFactory.createDataSource()
        listeners.forEach(next::addTransferListener)
        delegate = next
        return try { next.open(dataSpec) } catch (e: IOException) {
            runCatching { next.close() }
            delegate = null
            throw e
        }
    }
    override fun getUri(): Uri? = delegate?.uri ?: spec?.uri
    override fun getResponseHeaders(): Map<String, List<String>> {
        delegate?.let { return it.responseHeaders }
        val range = session ?: return emptyMap()
        val position = spec?.position ?: return emptyMap()
        if (range.totalLength < 0L) return emptyMap()
        return mapOf(
            "Content-Length" to listOf(range.resolvedLength.toString()),
            "Content-Range" to listOf("bytes " + position + "-" + (position + range.resolvedLength - 1L) + "/" + range.totalLength),
            "Accept-Ranges" to listOf("bytes"),
        )
    }
    private fun endTransfer() = synchronized(listenerLock) {
        val original = spec
        if (transferStarted && original != null) {
            transferStarted = false
            listeners.forEach { it.onTransferEnd(this, original, true) }
        }
    }
    override fun close() {
        session?.close()
        session = null
        endTransfer()
        runCatching { delegate?.close() }
        delegate = null
        spec = null
        delivered = 0L
        resolvedLength = C.LENGTH_UNSET.toLong()
    }
}
