package blbl.cat3399.feature.player.engine

import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class BilibiliCdnRoutesTest {
    private val original = "https://upos-sz-mirrorcosov.bilivideo.com/upgcxcode/1/2/3/3-1-30120.m4s?deadline=123&sign=a%2Fb%2Bc&x=1&x=2"

    @Test fun mainlandMode_addsRealMainlandNodesAndPreservesOriginalFallback() {
        val routes = BilibiliCdnRoutes.expand(listOf(original), "mainland")
        assertTrue(routes.any { it.toHttpUrl().host == "upos-sz-mirrorhw.bilivideo.com" })
        assertTrue(routes.any { it.toHttpUrl().host == "upos-sz-mirror08c.bilivideo.com" })
        assertTrue(original in routes)
        assertEquals(0, BilibiliCdnRoutes.regionRank(routes.first().toHttpUrl().host, "mainland"))
        routes.forEach {
            assertEquals(original.toHttpUrl().encodedPath, it.toHttpUrl().encodedPath)
            assertEquals(original.toHttpUrl().encodedQuery, it.toHttpUrl().encodedQuery)
        }
    }

    @Test fun classification_doesNotTreatCosovOrAliovAsMainland() {
        for (host in BilibiliCdnRoutes.overseasHosts + "upos-hz-mirrorakam.akamaized.net") {
            assertEquals(1, BilibiliCdnRoutes.regionRank(host, "mainland"))
            assertEquals(0, BilibiliCdnRoutes.regionRank(host, "overseas"))
        }
    }

    @Test fun overseasMode_doesNotSynthesizeMainlandNodes() {
        val routes = BilibiliCdnRoutes.expand(listOf(original), "overseas")
        assertFalse(routes.any { it.toHttpUrl().host in BilibiliCdnRoutes.mainlandHosts })
        assertTrue(routes.any { it.toHttpUrl().host == "upos-sz-mirroraliov.bilivideo.com" })
    }

    @Test fun autoMode_includesBothPoolsAndCanUseAnAkamaiOnlyDonor() {
        val akamai = original.replace("upos-sz-mirrorcosov.bilivideo.com", "upos-hz-mirrorakam.akamaized.net")
        val routes = BilibiliCdnRoutes.expand(listOf(akamai), "auto")
        assertTrue(akamai in routes)
        assertTrue(routes.any { it.toHttpUrl().host in BilibiliCdnRoutes.mainlandHosts })
        assertTrue(routes.any { it.toHttpUrl().host in BilibiliCdnRoutes.overseasHosts })
    }

    @Test fun arbitraryHostsCredentialsAndNonDashResourcesAreNotRewritten() {
        val rejected = listOf(
            original.replace("upos-sz-mirrorcosov.bilivideo.com", "example.com"),
            original.replace("upos-sz-mirrorcosov.bilivideo.com", "bilivideo.com.example.com"),
            original.replace("https://", "https://user:password@"),
            original.replace(".m4s?", ".m3u8?"),
            original.replace("/upgcxcode/", "/api/"),
        )
        rejected.forEach { assertEquals(listOf(it), BilibiliCdnRoutes.expand(listOf(it), "mainland")) }
    }

    @Test fun synthesizedUrlsUseHttpsWithoutPeerPort() {
        val peer = original.replace("https://", "http://").replace(".com/", ".com:4483/")
        val routes = BilibiliCdnRoutes.expand(listOf(peer), "mainland").filter { it != peer }
        assertTrue(routes.isNotEmpty())
        routes.forEach {
            assertEquals("https", it.toHttpUrl().scheme)
            assertEquals(443, it.toHttpUrl().port)
        }
    }
}
