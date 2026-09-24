@file:androidx.annotation.OptIn(markerClass = [androidx.media3.common.util.UnstableApi::class])

package blbl.cat3399.feature.player

import android.net.Uri
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.TransferListener
import java.io.IOException

internal enum class DebugStreamKind { VIDEO, AUDIO, MAIN }

internal class CdnFailoverState(
    val kind: DebugStreamKind,
    val candidates: List<Uri>,
) {
    @Volatile
    private var preferredIndex: Int = 0

    private var roundRobinCursor: Int = 0

    @Synchronized
    fun getPreferredIndex(): Int {
        val last = candidates.lastIndex
        return preferredIndex.coerceIn(0, last.coerceAtLeast(0))
    }

    @Synchronized
    fun prefer(index: Int) {
        val last = candidates.lastIndex
        preferredIndex = index.coerceIn(0, last.coerceAtLeast(0))
    }

    /**
     * Give concurrent range pieces different first candidates. A single shared preferred index
     * makes every piece race the same CDN and turns multi-range playback back into one route.
     */
    @Synchronized
    fun claimCandidateStartIndex(): Int {
        val last = candidates.lastIndex
        if (last < 0) return 0
        val start = (preferredIndex + roundRobinCursor) % (last + 1)
        roundRobinCursor = (roundRobinCursor + 1) % (last + 1)
        return start
    }
}

internal class CdnFailoverDataSourceFactory(
    private val upstreamFactory: DataSource.Factory,
    private val state: CdnFailoverState,
) : DataSource.Factory {
    override fun createDataSource(): DataSource = CdnFailoverDataSource(upstreamFactory, state)
}

internal class CdnFailoverDataSource(
    private val upstreamFactory: DataSource.Factory,
    private val state: CdnFailoverState,
) : DataSource {
    private var upstream: DataSource? = null
    private val transferListeners = ArrayList<TransferListener>(2)

    override fun addTransferListener(transferListener: TransferListener) {
        transferListeners.add(transferListener)
        upstream?.addTransferListener(transferListener)
    }

    override fun open(dataSpec: DataSpec): Long {
        closeQuietly()
        val candidates = state.candidates
        if (candidates.isEmpty()) throw IOException("No CDN candidates (kind=${state.kind})")

        val start = state.getPreferredIndex()
        var lastException: IOException? = null
        for (attempt in candidates.indices) {
            val idx = (start + attempt) % candidates.size
            val uri = candidates[idx]
            val ds = upstreamFactory.createDataSource()
            transferListeners.forEach { ds.addTransferListener(it) }
            val spec = dataSpec.buildUpon().setUri(uri).build()
            try {
                val openedLength = ds.open(spec)
                upstream = ds
                state.prefer(idx)
                return openedLength
            } catch (e: IOException) {
                runCatching { ds.close() }
                lastException = e
            }
        }
        throw lastException ?: IOException("Failed to open any CDN candidate (kind=${state.kind})")
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        val ds = upstream ?: throw IllegalStateException("read() before open() (kind=${state.kind})")
        return ds.read(buffer, offset, length)
    }

    override fun getUri(): Uri? = upstream?.uri

    override fun close() {
        closeQuietly()
    }

    private fun closeQuietly() {
        runCatching { upstream?.close() }
        upstream = null
    }
}
