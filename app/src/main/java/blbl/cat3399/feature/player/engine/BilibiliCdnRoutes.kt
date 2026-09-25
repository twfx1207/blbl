package blbl.cat3399.feature.player.engine

import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull

/**
 * The built-in node pool follows Bilibili-thread-ripper's cdn-resolver.js.
 * Only known Bilibili DASH media can be rewritten, and only to these fixed HTTPS hosts.
 * See assets/THIRD_PARTY_NOTICES.md for the upstream MIT notice.
 */
internal object BilibiliCdnRoutes {
    val mainlandHosts = listOf(
        "upos-sz-mirrorali.bilivideo.com",
        "upos-sz-mirrorhw.bilivideo.com",
        "upos-sz-mirror08c.bilivideo.com",
        "upos-sz-mirrorbos.bilivideo.com",
        "upos-sz-mirrorbd.bilivideo.com",
        "upos-sz-mirror14b.bilivideo.com",
        "upos-sz-estgoss.bilivideo.com",
        "upos-sz-mirrorcos.bilivideo.com",
    )
    val overseasHosts = listOf(
        "upos-sz-mirrorcosov.bilivideo.com",
        "upos-sz-mirroraliov.bilivideo.com",
        "cn-hk-eq-01-01.bilivideo.com",
        "cn-hk-eq-01-03.bilivideo.com",
    )
    private const val AKAMAI_HOST = "upos-hz-mirrorakam.akamaized.net"

    fun regionRank(host: String, mode: String): Int = when (mode) {
        "mainland" -> if (host in mainlandHosts) 0 else 1
        "overseas" -> if (host in overseasHosts || host == AKAMAI_HOST) 0 else 1
        else -> 0
    }

    private fun canRewrite(url: HttpUrl): Boolean =
        url.username.isEmpty() && url.password.isEmpty() &&
            (url.host.endsWith(".bilivideo.com") || url.host.endsWith(".bilivideo.cn") || url.host == AKAMAI_HOST) &&
            url.encodedPath.startsWith("/upgcxcode/") && url.encodedPath.endsWith(".m4s")

    fun expand(candidates: List<String>, mode: String): List<String> {
        val originals = candidates.map(String::trim).filter(String::isNotBlank).distinct()
        val donors = originals.mapNotNull { it.toHttpUrlOrNull() }.filter(::canRewrite)
            .sortedBy { it.host == AKAMAI_HOST }.take(2)
        if (donors.isEmpty()) return originals
        val hosts = when (mode) {
            "mainland" -> mainlandHosts
            "overseas" -> overseasHosts
            else -> mainlandHosts + overseasHosts
        }
        // Node-major order: different signatures must not fill all the early probe slots.
        val synthesized = hosts.flatMap { host ->
            donors.map { it.newBuilder().scheme("https").host(host).port(443).build().toString() }
        }
        val selectedOriginals = originals.filter { it.toHttpUrlOrNull()?.host in hosts }
        // Keep every original for safe fallback, including the URI used by Exo's routing map.
        return (selectedOriginals + synthesized + originals).distinct()
    }
}
