package blbl.cat3399.feature.player.engine

/** Immutable for a playback session. UI changes take effect on the next player instance. */
internal data class RangeDownloadOptions(
    val enabled: Boolean = true,
    val automatic: Boolean = true,
    val maxConnections: Int = 6,
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
 * A small, deterministic throughput trial controller. Only useful, non-duplicate bytes count.
 * Idle/full-buffer periods are not evidence that more connections would be faster.
 */
internal class RangeConcurrencyController(private val maximum: Int) {
    // Two video lanes plus one normal audio lane on the default TV configuration.
    var limit: Int = minOf(3, maximum)
        private set
    private var windowStart = 0L
    private var bytes = 0L
    private var baseline = 0.0
    private var trialFrom = 0
    private var restUntil = 0L
    private var saturated = false

    @Synchronized fun record(bytes: Int) { this.bytes += bytes }

    @Synchronized
    fun update(nowMs: Long, bufferMs: Long, busy: Boolean): Int {
        if (windowStart == 0L) windowStart = nowMs
        saturated = saturated || busy
        val elapsed = nowMs - windowStart
        if (elapsed < 2_000L) return limit
        val rate = bytes * 1000.0 / elapsed.coerceAtLeast(1L)
        val canMeasure = saturated && bytes >= 64 * 1024 && bufferMs < 20_000L
        if (canMeasure && trialFrom > 0) {
            if (rate < baseline * 1.1) {
                limit = trialFrom
                restUntil = nowMs + 10_000L
            }
            trialFrom = 0
        } else if (!canMeasure && trialFrom > 0) {
            limit = trialFrom
            trialFrom = 0
        } else if (canMeasure && bufferMs < 8_000L && nowMs >= restUntil && limit < maximum) {
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
