package blbl.cat3399.feature.player.engine

/** Immutable for a playback session. UI changes take effect on the next player instance. */
internal data class RangeDownloadOptions(
    val enabled: Boolean = true,
    val automatic: Boolean = true,
    val maxConnections: Int = 8,
    val cdnMode: String = "auto",
    val rescueEnabled: Boolean = true,
    val memoryMiB: Int = 4,
    val debug: Boolean = false,
) {
    val connectionLimit: Int get() = maxConnections.coerceIn(2, 8)
    val memoryBytes: Int get() = memoryMiB.coerceIn(2, 8) * 1024 * 1024
}

internal object ParallelRangeConfig {
    const val HEAD_BYTES = 64 * 1024
    const val MIN_PIECE_BYTES = 128 * 1024
    const val MAX_PIECE_BYTES = 512 * 1024
    const val CALL_TIMEOUT_MS = 8_000L
    const val STALL_MS = 900L
    const val CRITICAL_WAIT_MS = 10_000L
    const val MEASUREMENT_MIN_BYTES = 64L * 1024L
    const val MEASUREMENT_TTL_MS = 90_000L
}

/** Resource-specific confidence: a tiny init response is not a bandwidth measurement. */
internal class RangeRouteMeasurement {
    private var measuredSpeed = 0.0
    private var measuredAt = 0L
    var blockedUntil = 0L
        private set
    var firstByteMs = 0L
        private set
    private var failures = 0

    fun speed(nowMs: Long): Double =
        if (nowMs - measuredAt <= ParallelRangeConfig.MEASUREMENT_TTL_MS) measuredSpeed else 0.0

    fun record(bytes: Long, elapsedMs: Long, ttfbMs: Long, complete: Boolean, media: Boolean, nowMs: Long) {
        if (ttfbMs >= 0L) firstByteMs = if (firstByteMs == 0L) ttfbMs else (firstByteMs * 3L + ttfbMs) / 4L
        if (media && bytes >= ParallelRangeConfig.MEASUREMENT_MIN_BYTES && elapsedMs > 0L) {
            val sample = bytes * 1000.0 / elapsedMs
            val old = speed(nowMs)
            measuredSpeed = if (old == 0.0) sample else old * 0.65 + sample * 0.35
            measuredAt = nowMs
        }
        // A cancelled/losing hedge can provide a sample, but cannot clear a failure cooldown.
        if (complete) {
            failures = 0
            blockedUntil = 0L
        }
    }

    fun fail(nowMs: Long) {
        failures = (failures + 1).coerceAtMost(4)
        blockedUntil = nowMs + (3_000L shl failures).coerceAtMost(60_000L)
    }

    fun slow(nowMs: Long) {
        blockedUntil = maxOf(blockedUntil, nowMs + 3_000L)
    }
}

/** Plan only the next chunk, never allocate a whole media request. Endpoints are inclusive. */
internal data class ParallelByteRange(val start: Long, val end: Long) {
    init {
        require(start >= 0L && end >= start && end - start < Long.MAX_VALUE)
    }
    val length: Long get() = end - start + 1L
}

internal object ParallelRangePlanner {
    fun next(start: Long, end: Long, first: Boolean, bytesPerSecond: Double = 0.0): ParallelByteRange {
        require(start >= 0L && end >= start)
        val size =
            if (first) ParallelRangeConfig.HEAD_BYTES
            else (bytesPerSecond * 0.4).toInt().coerceIn(
                ParallelRangeConfig.MIN_PIECE_BYTES,
                ParallelRangeConfig.MAX_PIECE_BYTES,
            )
        return ParallelByteRange(start, start + minOf(end - start, size.toLong() - 1L))
    }
}

internal data class ParallelContentRange(val start: Long, val end: Long, val total: Long) {
    val length: Long get() = end - start + 1L
}

internal fun parseParallelContentRange(value: String?): ParallelContentRange? {
    val match = Regex("^bytes\\s+(\\d+)-(\\d+)/(\\d+)$", RegexOption.IGNORE_CASE)
        .matchEntire(value?.trim().orEmpty()) ?: return null
    val start = match.groupValues[1].toLongOrNull() ?: return null
    val end = match.groupValues[2].toLongOrNull() ?: return null
    val total = match.groupValues[3].toLongOrNull() ?: return null
    if (start < 0L || end < start || total <= end) return null
    return ParallelContentRange(start, end, total)
}

/**
 * Useful-byte trials with a playback safety floor. Starvation is not a safe time to reduce
 * capacity on the basis of one noisy throughput sample. Idle periods do not cause growth.
 */
internal class RangeConcurrencyController(private val maximum: Int) {
    // Four video lanes and one audio lane; one additional rescue lane lives outside this limit.
    var limit: Int = minOf(5, maximum)
        private set
    private var windowStart = 0L
    private var bytes = 0L
    private var baseline = 0.0
    private var trialFrom = 0
    private var restUntil = 0L
    private var saturated = false

    @Synchronized fun record(bytes: Int) { this.bytes += bytes }

    @Synchronized
    fun update(nowMs: Long, bufferMs: Long, busy: Boolean, requiredBytesPerSecond: Double = 0.0): Int {
        if (windowStart == 0L) windowStart = nowMs
        saturated = saturated || busy
        val elapsed = nowMs - windowStart
        if (elapsed >= 2_000L && saturated && bufferMs < 3_000L) {
            limit = maximum
            trialFrom = 0
        }
        if (elapsed < 4_000L) return limit
        val rate = bytes * 1000.0 / elapsed.coerceAtLeast(1L)
        val canMeasure = saturated && bytes >= 64 * 1024 && bufferMs < 20_000L
        val safeToReduce = bufferMs >= 8_000L &&
            (requiredBytesPerSecond <= 0.0 || rate >= requiredBytesPerSecond * 1.5)
        if (canMeasure && trialFrom > 0) {
            if (rate < baseline * 1.1 && safeToReduce) {
                limit = trialFrom
                restUntil = nowMs + 10_000L
            }
            trialFrom = 0
        } else if (!canMeasure && trialFrom > 0) {
            if (bufferMs >= 8_000L) limit = trialFrom
            trialFrom = 0
        } else if (canMeasure &&
            (bufferMs < 12_000L || rate < requiredBytesPerSecond * 1.5) &&
            nowMs >= restUntil && limit < maximum
        ) {
            baseline = rate
            trialFrom = limit
            limit++
        }
        bytes = 0L
        windowStart = nowMs
        saturated = false
        return limit
    }
}
