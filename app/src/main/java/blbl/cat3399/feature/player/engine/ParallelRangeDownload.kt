@file:androidx.annotation.OptIn(markerClass = [androidx.media3.common.util.UnstableApi::class])

package blbl.cat3399.feature.player.engine

import android.net.Uri
import androidx.media3.common.C
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.TransferListener
import blbl.cat3399.core.log.AppLog
import blbl.cat3399.feature.player.CdnFailoverState
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.Closeable
import java.io.IOException
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil

/** A bounded byte range with inclusive endpoints. */
internal data class ParallelByteRange(
    val start: Long,
    val end: Long,
) {
    init {
        require(start >= 0L) { "range start must be non-negative" }
        require(end >= start) { "range end must not precede start" }
    }

    val length: Long
        get() = end - start + 1L
}

internal object ParallelRangePlanner {
    fun split(
        start: Long,
        length: Long,
        maxPieces: Int = ParallelRangeConfig.MAX_PIECES,
        minPieceBytes: Long = ParallelRangeConfig.MIN_PIECE_BYTES,
    ): List<ParallelByteRange> {
        require(start >= 0L) { "range start must be non-negative" }
        require(length > 0L) { "range length must be positive" }
        val safeMaxPieces = maxPieces.coerceIn(1, ParallelRangeConfig.MAX_PIECES)
        val safeMinimum = minPieceBytes.coerceAtLeast(32L * 1024L)
        val count =
            ceil(length.toDouble() / safeMinimum.toDouble())
                .toInt()
                .coerceIn(1, safeMaxPieces)
        val base = length / count.toLong()
        val remainder = (length % count.toLong()).toInt()
        val result = ArrayList<ParallelByteRange>(count)
        var cursor = start
        repeat(count) { index ->
            val pieceLength = base + if (index < remainder) 1L else 0L
            val end = cursor + pieceLength - 1L
            result += ParallelByteRange(start = cursor, end = end)
            cursor = end + 1L
        }
        return result
    }
}

internal data class ParallelContentRange(
    val start: Long,
    val end: Long,
    val total: Long,
) {
    val length: Long
        get() = end - start + 1L
}

internal fun parseParallelContentRange(value: String?): ParallelContentRange? {
    val raw = value?.trim().orEmpty()
    if (raw.isBlank()) return null
    val match = Regex("^bytes\\s+(\\d+)-(\\d+)/(\\d+|\\*)$", RegexOption.IGNORE_CASE).matchEntire(raw) ?: return null
    val start = match.groupValues[1].toLongOrNull() ?: return null
    val end = match.groupValues[2].toLongOrNull() ?: return null
    val total = match.groupValues[3].toLongOrNull() ?: return null
    if (start < 0L || end < start || total <= end) return null
    return ParallelContentRange(start = start, end = end, total = total)
}

/**
 * Small per-player CDN memory. A URL that delivers data is preferred next time, while a
 * repeatedly empty response is temporarily cooled down. The first piece of each range still
 * rotates through the candidates so a fast node is discovered instead of being assumed.
 */
internal class CdnSpeedTracker {
    private data class Entry(
        var bytesPerSecond: Double = 0.0,
        var failures: Int = 0,
        var blockedUntilMs: Long = 0L,
    )

    private val entries = ConcurrentHashMap<String, Entry>()

    fun ordered(candidates: List<Uri>, startIndex: Int, nowMs: Long = System.currentTimeMillis()): List<Uri> {
        if (candidates.isEmpty()) return emptyList()
        val rotated =
            candidates.indices
                .map { offset -> candidates[(startIndex + offset) % candidates.size] }
        val available = rotated.filter { entry(it, nowMs).blockedUntilMs <= nowMs }
        val pool = available.ifEmpty { rotated }
        val primary = pool.firstOrNull() ?: return emptyList()
        return listOf(primary) +
            pool
                .drop(1)
                .sortedWith(
                    compareByDescending<Uri> { entry(it, nowMs).bytesPerSecond }
                        .thenBy { it.toString() },
                )
    }

    fun recordSuccess(uri: Uri, bytes: Long, elapsedMs: Long) {
        if (bytes <= 0L || elapsedMs <= 0L) return
        val current = entry(uri, System.currentTimeMillis())
        val bps = bytes.toDouble() * 1000.0 / elapsedMs.toDouble()
        synchronized(current) {
            current.bytesPerSecond =
                if (current.bytesPerSecond > 0.0) {
                    current.bytesPerSecond * 0.7 + bps * 0.3
                } else {
                    bps
                }
            current.failures = 0
            current.blockedUntilMs = 0L
        }
    }

    fun recordFailure(uri: Uri, receivedBytes: Long) {
        if (receivedBytes > 0L) return
        val current = entry(uri, System.currentTimeMillis())
        synchronized(current) {
            current.failures = (current.failures + 1).coerceAtMost(8)
            val backoffMs = (3_000L * (1L shl (current.failures - 1).coerceIn(0, 4))).coerceAtMost(60_000L)
            current.blockedUntilMs = System.currentTimeMillis() + backoffMs
        }
    }

    fun reset() {
        entries.clear()
    }

    private fun entry(uri: Uri, nowMs: Long): Entry {
        val key = routeKey(uri)
        val value = entries.getOrPut(key) { Entry() }
        synchronized(value) {
            if (value.blockedUntilMs <= nowMs) value.blockedUntilMs = 0L
        }
        return value
    }

    private fun routeKey(uri: Uri): String =
        buildString {
            append(uri.host.orEmpty().lowercase(Locale.US))
            append(uri.encodedPath.orEmpty())
        }
}

internal object ParallelRangeConfig {
    const val MIN_ACCELERATED_RANGE_BYTES: Long = 256L * 1024L
    const val MIN_PIECE_BYTES: Long = 128L * 1024L
    const val MAX_PIECES: Int = 8
    const val MAX_ACCELERATED_RANGE_BYTES: Long = 64L * 1024L * 1024L
    const val MAX_ATTEMPTS_PER_PIECE: Int = 4
    const val CALL_TIMEOUT_MS: Long = 30_000L
    const val DEFAULT_CONCURRENCY: Int = 8
}

internal class ParallelRangeScheduler(
    maxConcurrency: Int = ParallelRangeConfig.DEFAULT_CONCURRENCY,
) : Closeable {
    private val threadCounter = AtomicInteger(0)
    private val executor: ExecutorService =
        Executors.newFixedThreadPool(
            maxConcurrency.coerceIn(1, ParallelRangeConfig.MAX_PIECES),
            ThreadFactory { runnable ->
                Thread(runnable, "blbl-range-${threadCounter.incrementAndGet()}").apply {
                    isDaemon = true
                }
            },
        )

    fun <T> submit(task: Callable<T>): Future<T> = executor.submit(task)

    override fun close() {
        executor.shutdownNow()
        runCatching { executor.awaitTermination(800L, TimeUnit.MILLISECONDS) }
    }
}

private class RangeCancellation {
    private val canceled = AtomicBoolean(false)
    private val calls = CopyOnWriteArrayList<Call>()

    fun register(call: Call) {
        if (canceled.get()) {
            call.cancel()
        } else {
            calls += call
            if (canceled.get()) call.cancel()
        }
    }

    fun unregister(call: Call) {
        calls.remove(call)
    }

    fun cancel() {
        if (!canceled.compareAndSet(false, true)) return
        calls.forEach { it.cancel() }
        calls.clear()
    }

    fun throwIfCanceled() {
        if (canceled.get() || Thread.currentThread().isInterrupted) {
            throw IOException("parallel range download cancelled")
        }
    }
}

internal data class ParallelRangePieceResult(
    val bytes: ByteArray,
    val totalLength: Long,
    val url: Uri,
)

internal class ParallelRangeSession(
    private val client: OkHttpClient,
    candidates: List<Uri>,
    private val state: CdnFailoverState,
    private val scheduler: ParallelRangeScheduler,
    private val speedTracker: CdnSpeedTracker,
    start: Long,
    length: Long,
    private val requestHeaders: Map<String, String> = emptyMap(),
    private val onHost: ((String) -> Unit)? = null,
    private val onNetworkBytes: ((Long) -> Unit)? = null,
) : Closeable {
    val pieces: List<ParallelByteRange> = ParallelRangePlanner.split(start = start, length = length)

    private val candidateUris = candidates.distinct()
    private val cancellation = RangeCancellation()
    private val expectedTotal = AtomicLong(-1L)
    private val futures: List<Future<ParallelRangePieceResult>>

    @Volatile
    private var closed = false

    init {
        require(candidateUris.isNotEmpty()) { "parallel range download needs a CDN candidate" }
        futures =
            pieces.mapIndexed { index, piece ->
                scheduler.submit(Callable { downloadPiece(index = index, piece = piece) })
            }
    }

    fun awaitPiece(index: Int): ParallelRangePieceResult {
        if (index !in futures.indices) throw IndexOutOfBoundsException("piece=$index size=${futures.size}")
        if (closed) throw IOException("parallel range session closed")
        return try {
            futures[index].get()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IOException("parallel range wait interrupted", e)
        } catch (e: ExecutionException) {
            val cause = e.cause
            throw when (cause) {
                is IOException -> cause
                else -> IOException("parallel range piece failed", cause)
            }
        }
    }

    override fun close() {
        if (closed) return
        closed = true
        cancellation.cancel()
        futures.forEach { future -> future.cancel(true) }
    }

    private fun downloadPiece(index: Int, piece: ParallelByteRange): ParallelRangePieceResult {
        val candidatesInOrder = candidateOrder()
        val output = ByteArray(piece.length.toInt())
        var received = 0
        var attempt = 0
        var lastError: IOException? = null
        val pieceStartedAtNs = System.nanoTime()

        while (received < output.size && attempt < ParallelRangeConfig.MAX_ATTEMPTS_PER_PIECE) {
            cancellation.throwIfCanceled()
            val candidate = candidatesInOrder[attempt % candidatesInOrder.size]
            val requestStart = piece.start + received.toLong()
            val expectedLength = output.size - received
            var attemptReceived = 0L
            val call =
                client.newCall(
                    buildRequest(
                        uri = candidate,
                        start = requestStart,
                        end = piece.end,
                    ),
                )
            cancellation.register(call)
            try {
                call.timeout().timeout(ParallelRangeConfig.CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                call.execute().use { response ->
                    val contentRange = parseParallelContentRange(response.header("Content-Range"))
                    if (response.code != 206 || contentRange == null) {
                        throw IOException("range response invalid http=${response.code} url=${candidate.host}")
                    }
                    if (contentRange.start != requestStart || contentRange.end != piece.end || contentRange.length != expectedLength) {
                        throw IOException(
                            "range response mismatch expected=$requestStart-${piece.end} " +
                                "actual=${contentRange.start}-${contentRange.end}",
                        )
                    }
                    val contentLength =
                        response.header("Content-Length")?.toLongOrNull()
                            ?: response.body?.contentLength()
                            ?: -1L
                    if (contentLength >= 0L && contentLength != expectedLength.toLong()) {
                        throw IOException("range body length mismatch expected=$expectedLength actual=$contentLength")
                    }
                    val body = response.body ?: throw IOException("range response has no body")
                    body.byteStream().use { input ->
                        while (received < output.size) {
                            cancellation.throwIfCanceled()
                            val count = input.read(output, received, output.size - received)
                            if (count < 0) throw IOException("range body ended at $received/${output.size}")
                            if (count == 0) {
                                Thread.yield()
                                continue
                            }
                            received += count
                            attemptReceived += count.toLong()
                            onNetworkBytes?.invoke(count.toLong())
                        }
                    }
                    val currentTotal = expectedTotal.get()
                    if (currentTotal > 0L && currentTotal != contentRange.total) {
                        throw IOException("CDN total length mismatch expected=$currentTotal actual=${contentRange.total}")
                    }
                    expectedTotal.compareAndSet(-1L, contentRange.total)
                    if (expectedTotal.get() != contentRange.total) {
                        throw IOException("CDN total length changed during range download")
                    }
                    val elapsedMs = ((System.nanoTime() - pieceStartedAtNs) / 1_000_000L).coerceAtLeast(1L)
                    speedTracker.recordSuccess(candidate, piece.length, elapsedMs)
                    state.prefer(stateIndexOf(candidate))
                    onHost?.invoke(candidate.host.orEmpty())
                    return ParallelRangePieceResult(bytes = output, totalLength = contentRange.total, url = candidate)
                }
            } catch (e: IOException) {
                if (closed) throw e
                lastError = e
                speedTracker.recordFailure(candidate, receivedBytes = attemptReceived)
                attempt++
            } finally {
                cancellation.unregister(call)
            }
        }
        throw lastError ?: IOException("parallel range piece failed index=$index")
    }

    private fun candidateOrder(): List<Uri> {
        val startIndex = state.claimCandidateStartIndex()
        return speedTracker.ordered(candidateUris, startIndex).ifEmpty { candidateUris }
    }

    private fun stateIndexOf(uri: Uri): Int {
        val index = state.candidates.indexOfFirst { it == uri }
        return index.takeIf { it >= 0 } ?: 0
    }

    private fun buildRequest(uri: Uri, start: Long, end: Long): Request {
        val builder = Request.Builder().url(uri.toString())
        requestHeaders.forEach { (name, value) ->
            if (
                name.isNotBlank() &&
                !name.equals("Range", ignoreCase = true) &&
                !name.equals("Accept-Encoding", ignoreCase = true)
            ) {
                builder.header(name, value)
            }
        }
        return builder
            .header("Range", "bytes=$start-$end")
            .header("Accept-Encoding", "identity")
            .get()
            .build()
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
) : DataSource.Factory {
    override fun createDataSource(): DataSource =
        ParallelRangeDataSource(
            fallbackFactory = fallbackFactory,
            client = client,
            candidates = candidates,
            state = state,
            scheduler = scheduler,
            speedTracker = speedTracker,
            onTransferHost = onTransferHost,
        )
}

private class ParallelRangeDataSource(
    private val fallbackFactory: DataSource.Factory,
    private val client: OkHttpClient,
    private val candidates: List<Uri>,
    private val state: CdnFailoverState,
    private val scheduler: ParallelRangeScheduler,
    private val speedTracker: CdnSpeedTracker,
    private val onTransferHost: ((String) -> Unit)?,
) : DataSource {
    private val transferListeners = ArrayList<TransferListener>(2)
    private var delegate: DataSource? = null
    private var session: ParallelRangeSession? = null
    private var sourceSpec: DataSpec? = null
    private var currentPiece: ParallelRangePieceResult? = null
    private var currentPieceOffset: Int = 0
    private var nextPieceIndex: Int = 0
    private var deliveredBytes: Long = 0L
    private var transferStarted = false
    private var fallbackActivated = false

    override fun addTransferListener(transferListener: TransferListener) {
        transferListeners += transferListener
        delegate?.addTransferListener(transferListener)
    }

    override fun open(dataSpec: DataSpec): Long {
        close()
        sourceSpec = dataSpec
        if (!isEligible(dataSpec)) return openFallback(dataSpec)

        val rangeCandidates = candidates.ifEmpty { listOf(dataSpec.uri) }
        val rangeSession =
            runCatching {
                ParallelRangeSession(
                    client = client,
                    candidates = rangeCandidates,
                    state = state,
                    scheduler = scheduler,
                    speedTracker = speedTracker,
                    start = dataSpec.position,
                    length = dataSpec.length,
                    requestHeaders = dataSpec.httpRequestHeaders,
                    onHost = onTransferHost,
                )
            }.getOrElse {
                return openFallback(dataSpec)
            }
        session = rangeSession
        transferListeners.forEach { it.onTransferInitializing(this, dataSpec, true) }
        return try {
            rangeSession.awaitPiece(0)
            transferListeners.forEach { it.onTransferStart(this, dataSpec, true) }
            transferStarted = true
            dataSpec.length
        } catch (throwable: Throwable) {
            rangeSession.close()
            session = null
            notifyParallelTransferEnd(dataSpec)
            openFallback(dataSpec)
        }
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        val fallback = delegate
        if (fallback != null) return fallback.read(buffer, offset, length)
        if (length == 0) return 0
        val rangeSession = session ?: throw IllegalStateException("read() before open()")
        while (true) {
            val piece = currentPiece
            if (piece != null && currentPieceOffset < piece.bytes.size) {
                val count = minOf(length, piece.bytes.size - currentPieceOffset)
                piece.bytes.copyInto(buffer, destinationOffset = offset, startIndex = currentPieceOffset, endIndex = currentPieceOffset + count)
                currentPieceOffset += count
                deliveredBytes += count.toLong()
                val spec = sourceSpec ?: throw IllegalStateException("missing source spec")
                transferListeners.forEach { it.onBytesTransferred(this, spec, true, count) }
                if (currentPieceOffset >= piece.bytes.size) currentPiece = null
                return count
            }

            if (nextPieceIndex >= rangeSession.pieces.size) return C.RESULT_END_OF_INPUT
            try {
                currentPiece = rangeSession.awaitPiece(nextPieceIndex++)
                currentPieceOffset = 0
            } catch (throwable: Throwable) {
                return if (activateFallback(throwable)) {
                    checkNotNull(delegate).read(buffer, offset, length)
                } else {
                    throw asIOException(throwable)
                }
            }
        }
    }

    override fun getUri(): Uri? = delegate?.uri ?: sourceSpec?.uri

    override fun close() {
        val spec = sourceSpec
        session?.close()
        session = null
        if (spec != null) notifyParallelTransferEnd(spec)
        runCatching { delegate?.close() }
        delegate = null
        sourceSpec = null
        currentPiece = null
        currentPieceOffset = 0
        nextPieceIndex = 0
        deliveredBytes = 0L
        fallbackActivated = false
    }

    private fun activateFallback(throwable: Throwable): Boolean {
        if (fallbackActivated) return false
        val spec = sourceSpec ?: return false
        fallbackActivated = true
        session?.close()
        session = null
        notifyParallelTransferEnd(spec)
        val remaining = (spec.length - deliveredBytes).coerceAtLeast(0L)
        val fallbackSpec =
            spec
                .buildUpon()
                .setPosition(spec.position + deliveredBytes)
                .setLength(remaining)
                .build()
        return runCatching {
            val next = fallbackFactory.createDataSource()
            transferListeners.forEach(next::addTransferListener)
            next.open(fallbackSpec)
            delegate = next
        }.onFailure {
            AppLog.w("RangeAccel", "parallel download failed and single-connection fallback failed", throwable)
        }.isSuccess
    }

    private fun openFallback(dataSpec: DataSpec): Long {
        fallbackActivated = true
        val next = fallbackFactory.createDataSource()
        transferListeners.forEach(next::addTransferListener)
        delegate = next
        return try {
            next.open(dataSpec)
        } catch (throwable: Throwable) {
            delegate = null
            runCatching { next.close() }
            throw throwable
        }
    }

    private fun notifyParallelTransferEnd(dataSpec: DataSpec) {
        if (!transferStarted) return
        transferListeners.forEach { it.onTransferEnd(this, dataSpec, true) }
        transferStarted = false
    }

    private fun isEligible(dataSpec: DataSpec): Boolean =
        !fallbackActivated &&
            (dataSpec.uri.scheme.equals("http", ignoreCase = true) || dataSpec.uri.scheme.equals("https", ignoreCase = true)) &&
            dataSpec.httpMethod == DataSpec.HTTP_METHOD_GET &&
            dataSpec.httpBody == null &&
            dataSpec.length >= ParallelRangeConfig.MIN_ACCELERATED_RANGE_BYTES &&
            dataSpec.length <= ParallelRangeConfig.MAX_ACCELERATED_RANGE_BYTES &&
            dataSpec.length <= Int.MAX_VALUE.toLong() * ParallelRangeConfig.MAX_PIECES.toLong()

    private fun asIOException(throwable: Throwable): IOException =
        when (throwable) {
            is IOException -> throwable
            else -> IOException("parallel range read failed", throwable)
        }
}
