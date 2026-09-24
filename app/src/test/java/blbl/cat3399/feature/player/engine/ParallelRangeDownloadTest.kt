package blbl.cat3399.feature.player.engine

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ParallelRangeDownloadTest {
    @Test
    fun planner_shouldCoverRequestedRangeInOrderWithoutGaps() {
        val pieces =
            ParallelRangePlanner.split(
                start = 100L,
                length = 1_000_000L,
                maxPieces = 4,
                minPieceBytes = 128L * 1024L,
            )

        assertEquals(4, pieces.size)
        assertEquals(100L, pieces.first().start)
        assertEquals(1_000_099L, pieces.last().end)
        assertEquals(1_000_000L, pieces.sumOf { it.length })
        pieces.zipWithNext().forEach { (left, right) ->
            assertEquals(left.end + 1L, right.start)
        }
    }

    @Test
    fun planner_shouldNotCreateTinyPiecesWhenRangeIsSmallerThanMinimum() {
        val pieces = ParallelRangePlanner.split(start = 0L, length = 256L * 1024L, maxPieces = 8, minPieceBytes = 128L * 1024L)

        assertEquals(2, pieces.size)
        assertTrue(pieces.all { it.length >= 128L * 1024L })
    }

    @Test
    fun contentRangeParser_shouldRejectWildcardOrInconsistentTotal() {
        assertEquals(10L, parseParallelContentRange("bytes 100-109/1000")?.length)
        assertNull(parseParallelContentRange("bytes 100-109/*"))
        assertNull(parseParallelContentRange("bytes 100-109/109"))
        assertNull(parseParallelContentRange("bytes 109-100/1000"))
    }

}
