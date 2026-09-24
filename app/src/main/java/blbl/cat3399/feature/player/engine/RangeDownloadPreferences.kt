package blbl.cat3399.feature.player.engine

import android.content.Context
import blbl.cat3399.core.prefs.AppPrefs

internal fun loadRangeDownloadOptions(context: Context): RangeDownloadOptions =
    AppPrefs(context).let {
        RangeDownloadOptions(
            enabled = it.rangeDownloadEnabled,
            automatic = it.rangeDownloadAutomatic,
            maxConnections = it.rangeDownloadConnections,
            cdnMode = it.rangeDownloadCdnMode,
            rescueEnabled = it.rangeDownloadRescue,
            memoryMiB = it.rangeDownloadMemoryMiB,
            debug = it.rangeDownloadDebug,
        )
    }
