package blbl.cat3399.feature.player.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ParallelRangeDownloadTest {
    @Test fun firstChunk_staysSmallEvenForAnOpenEndedRequest() {
        val range = ParallelRangePlanner.next(100L, Long.MAX_VALUE - 1L, first = true, bytesPerSecond = 50_000_000.0)
        assertEquals(100L, range.start)
        assertEquals(64L * 1024L, range.length)
    }
    @Test fun window_coversRangeWithBoundedChunksAndNoGaps() {
        val end = 64L * 1024L * 1024L + 17L
        var cursor = 100L
        var first = true
        while (cursor <= end) {
            val range = ParallelRangePlanner.next(cursor, end, first, bytesPerSecond = 50_000_000.0)
            assertEquals(cursor, range.start)
            assertTrue(range.length in 1L..(512L * 1024L))
            cursor = range.end + 1L
            first = false
        }
        assertEquals(end + 1L, cursor)
    }
    @Test fun shortFinalChunk_doesNotReadBeyondTheResource() {
        assertEquals(ParallelByteRange(100L, 110L), ParallelRangePlanner.next(100L, 110L, first = true))
    }
    @Test fun parser_rejectsWildcardOverflowAndInconsistentTotal() {
        assertEquals(10L, parseParallelContentRange("bytes 100-109/1000")?.length)
        assertNull(parseParallelContentRange("bytes 100-109/*"))
        assertNull(parseParallelContentRange("bytes 100-109/109"))
        assertNull(parseParallelContentRange("bytes 109-100/1000"))
        assertNull(parseParallelContentRange("bytes 0-1/999999999999999999999999"))
    }
    @Test fun autoConcurrency_keepsRecoveryCapacityWhenBufferIsEmpty() {
        val controller = RangeConcurrencyController(maximum = 7)
        assertEquals(5, controller.update(1_000L, 0L, true))
        controller.record(1_000_000)
        assertEquals(7, controller.update(3_000L, 0L, true))
        controller.record(900_000)
        assertEquals(7, controller.update(5_000L, 293L, true))
        controller.record(1_000_000)
        assertEquals(7, controller.update(9_000L, 0L, true))
    }
    @Test fun autoConcurrency_revertsUnhelpfulTrialOnlyWithBufferAndRateMargin() {
        val controller = RangeConcurrencyController(maximum = 7)
        controller.update(1_000L, 9_000L, true, 500_000.0)
        controller.record(4_000_000)
        assertEquals(6, controller.update(5_000L, 9_000L, true, 500_000.0))
        controller.record(3_800_000)
        assertEquals(5, controller.update(9_000L, 9_000L, true, 500_000.0))
    }
    @Test fun autoConcurrency_doesNotReduceBelowPlaybackThroughputMargin() {
        val controller = RangeConcurrencyController(maximum = 7)
        controller.update(1_000L, 9_000L, true, 800_000.0)
        controller.record(4_000_000)
        assertEquals(6, controller.update(5_000L, 9_000L, true, 800_000.0))
        controller.record(3_800_000)
        assertEquals(6, controller.update(9_000L, 9_000L, true, 800_000.0))
    }
    @Test fun autoConcurrency_doesNotGrowWhenBufferIsFullOrDownloadIsIdle() {
        val controller = RangeConcurrencyController(maximum = 5)
        controller.update(1_000L, 30_000L, false)
        controller.record(1_000_000)
        assertEquals(5, controller.update(5_000L, 30_000L, false))
        assertEquals(5, controller.update(9_000L, 0L, false))
    }
    @Test fun options_clampImportedOrInvalidValuesToTvBounds() {
        val options = RangeDownloadOptions(maxConnections = 128, memoryMiB = 100)
        assertEquals(8, options.connectionLimit)
        assertEquals(8 * 1024 * 1024, options.memoryBytes)
    }

    @Test fun routeMeasurement_ignoresMetadataAndTinyTails() {
        val route = RangeRouteMeasurement()
        route.record(3_848L, 20L, 10L, complete = true, media = false, nowMs = 1_000L)
        route.record(128L * 1024L, 100L, 10L, complete = true, media = false, nowMs = 1_100L)
        route.record(1_024L, 20L, 10L, complete = true, media = true, nowMs = 1_200L)
        assertEquals(0.0, route.speed(1_200L), 0.0)
        route.record(65_536L, 100L, 10L, complete = true, media = true, nowMs = 1_300L)
        assertEquals(655_360.0, route.speed(1_300L), 0.0)
        assertEquals(0.0, route.speed(100_000L), 0.0)
    }

    @Test fun routeMeasurement_losingHedgeDoesNotClearCooldown() {
        val route = RangeRouteMeasurement()
        route.fail(1_000L)
        val blockedUntil = route.blockedUntil
        route.record(65_536L, 100L, 10L, complete = false, media = true, nowMs = 1_500L)
        assertEquals(blockedUntil, route.blockedUntil)
        assertTrue(route.speed(1_500L) > 0.0)
        route.record(65_536L, 100L, 10L, complete = true, media = true, nowMs = 2_000L)
        assertEquals(0L, route.blockedUntil)
    }
}
