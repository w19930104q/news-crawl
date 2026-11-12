/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commoncrawl.stormcrawler.news;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import crawlercommons.sitemaps.UnknownFormatException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.DefaultScheduler;
import org.apache.stormcrawler.persistence.Status;
import org.commoncrawl.stormcrawler.news.NewsSiteMapParserBolt.SiteMapParseResult;
import org.commoncrawl.stormcrawler.news.NewsSiteMapParserBolt.SitemapType;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for the canonical-article logic of {@link NewsSiteMapParserBolt}:
 *
 * <ul>
 *   <li>xhtml:link language alternates and duplicate {@code <loc>} entries are merged into one
 *       canonical article instead of being counted as separate news;
 *   <li>the effective article date is the newest valid {@code news:publication_date}, falling back
 *       to the newest valid {@code lastmod};
 *   <li>{@code numLinks} only counts actual new (recent, canonical) articles, so an unchanged old
 *       sitemap reports zero and becomes eligible for staleness retirement;
 *   <li>recursive sitemap indexes inherit the news type, verified flag and scheduling delay, and a
 *       single broken child entry does not lose the rest of the batch.
 * </ul>
 */
public class NewsSiteMapParserBoltTest extends ParsingTester {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Before
    public void setupParserBolt() {
        setupParserBolt(new NewsSiteMapParserBolt());
        Map<String, Object> config = new HashMap<>();
        config.put("sitemap.sniffContent", true);
        // allow items published during the last week (168h)
        config.put("sitemap.filter.hours.since.modified", 168);
        prepareParserBolt("test.parsefilters.json", config);
    }

    private byte[] fixture(String filename) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getClass().getClassLoader().getResourceAsStream(filename), baos);
        String xml =
                baos.toString(StandardCharsets.UTF_8)
                        .replace("@YESTERDAY@", LocalDate.now().minusDays(1).format(DATE))
                        .replace("@TWODAYSAGO@", LocalDate.now().minusDays(2).format(DATE))
                        // 30 days ago is well outside the 168h (7 days) recency window
                        .replace("@THIRTYDAYSAGO@", LocalDate.now().minusDays(30).format(DATE));
        return xml.getBytes(StandardCharsets.UTF_8);
    }

    private SiteMapParseResult parseFixture(String url, String filename, Metadata parentMetadata)
            throws IOException, UnknownFormatException {
        byte[] content = fixture(filename);
        return ((NewsSiteMapParserBolt) bolt)
                .parseSiteMapWithResult(
                        url,
                        content,
                        "",
                        parentMetadata != null ? parentMetadata : new Metadata(),
                        new ArrayList<>());
    }

    private SiteMapParseResult parseInto(
            String url, String filename, Metadata parentMetadata, List<Outlink> links)
            throws IOException, UnknownFormatException {
        byte[] content = fixture(filename);
        return ((NewsSiteMapParserBolt) bolt)
                .parseSiteMapWithResult(
                        url,
                        content,
                        "",
                        parentMetadata != null ? parentMetadata : new Metadata(),
                        links);
    }

    private static Set<String> targets(List<Outlink> links) {
        return links.stream().map(Outlink::getTargetURL).collect(Collectors.toSet());
    }

    // ------------------------------------------------------------------
    // multilingual alternates: one canonical article per logical story
    // ------------------------------------------------------------------

    @Test
    public void multilingualAlternates_mergedIntoCanonicalArticles() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemap-news-multilingual.xml",
                        "sitemap-news-multilingual.xml",
                        new Metadata(),
                        links);

        // two logical articles: the merger (EN+DE+FR) and the standalone FR election story
        assertEquals("only canonical recent articles count", 2, result.numCanonicalLinks);

        Set<String> emitted = targets(links);
        // every language variant of the merger is still emitted once as an outlink
        assertTrue(emitted.contains("http://www.example.com/business/merger.html"));
        assertTrue(emitted.contains("http://www.example.com/de/business/merger.html"));
        assertTrue(emitted.contains("http://www.example.com/fr/business/merger.html"));
        assertTrue(emitted.contains("http://www.example.com/fr/news/election.html"));
        // but no URL is emitted twice
        assertEquals("no duplicate outlinks", emitted.size(), links.size());
    }

    @Test
    public void multilingualAlternates_detectedAsNewsSitemap() throws Exception {
        byte[] content = fixture("sitemap-news-multilingual.xml");
        SitemapType type =
                ((NewsSiteMapParserBolt) bolt)
                        .detectContent(
                                "https://example.org/sitemap-news-multilingual.xml", content);
        assertEquals(SitemapType.NEWS, type);
    }

    // ------------------------------------------------------------------
    // duplicate loc: collapse, newest valid publication date wins
    // ------------------------------------------------------------------

    @Test
    public void duplicateLoc_collapsedToSingleCanonicalArticle() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemap-news-duplicate-loc.xml",
                        "sitemap-news-duplicate-loc.xml",
                        new Metadata(),
                        links);

        // two distinct articles even though three <url> entries were present
        assertEquals(2, result.numCanonicalLinks);
        assertTrue(result.numDuplicates >= 1);
        assertEquals(3, result.numEntriesFound);

        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/news/duplicate.html"));
        assertTrue(emitted.contains("http://www.example.com/news/unique.html"));
        assertEquals(2, emitted.size());
    }

    // ------------------------------------------------------------------
    // conflicting / missing dates
    // ------------------------------------------------------------------

    @Test
    public void conflictingDates_invalidPubDateFallsBackToLastmodAndValidAlternate()
            throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemap-news-conflicting-dates.xml",
                        "sitemap-news-conflicting-dates.xml",
                        new Metadata(),
                        links);

        // invaliddates (valid DE publication date) + lastmodonly (recent lastmod):
        // the stale article must be filtered out
        assertEquals(2, result.numCanonicalLinks);
        assertEquals(1, result.numSkippedNotRecent);

        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/news/invaliddates.html"));
        assertTrue(emitted.contains("http://www.example.com/de/news/invaliddates.html"));
        assertTrue(emitted.contains("http://www.example.com/news/lastmodonly.html"));
        assertFalse(emitted.contains("http://www.example.com/news/stale.html"));
    }

    // ------------------------------------------------------------------
    // mixed new/old sitemap: numLinks only counts actual new articles
    // ------------------------------------------------------------------

    @Test
    public void mixedNewAndOld_onlyRecentCanonicalArticlesCount() throws Exception {
        SiteMapParseResult result =
                parseFixture(
                        "https://example.org/sitemap-news-mixed-new-old.xml",
                        "sitemap-news-mixed-new-old.xml",
                        new Metadata());

        assertEquals(2, result.numCanonicalLinks);
        assertEquals(2, result.numSkippedNotRecent);
        assertEquals(4, result.numEntriesFound);
    }

    @Test
    public void allStaleSitemap_reportsZeroNumLinks() throws Exception {
        // re-use the conflicting-dates fixture but with the recency window
        // narrowed so every entry (including the recent ones) is stale: an
        // unchanged old sitemap must report zero links, not its entry count
        Map<String, Object> config = new HashMap<>();
        config.put("sitemap.sniffContent", true);
        config.put("sitemap.filter.hours.since.modified", 0);
        prepareParserBolt("test.parsefilters.json", config);

        SiteMapParseResult result =
                parseFixture(
                        "https://example.org/sitemap-news-mixed-new-old.xml",
                        "sitemap-news-mixed-new-old.xml",
                        new Metadata());
        assertEquals("stale sitemap must not look active", 0, result.numCanonicalLinks);
    }

    // ------------------------------------------------------------------
    // partially broken XML: surviving entries are still emitted
    // ------------------------------------------------------------------

    @Test
    public void partialBrokenUrlset_survivingArticlesStillEmitted() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemap-news-partial-broken.xml",
                        "sitemap-news-partial-broken.xml",
                        new Metadata(),
                        links);

        assertEquals(2, result.numCanonicalLinks);
        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/news/good-1.html"));
        assertTrue(emitted.contains("http://www.example.com/news/good-2.html"));
    }

    @Test
    public void malformedEntry_doesNotLoseTheRestOfTheBatch() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemap-news-bad-entry.xml",
                        "sitemap-news-bad-entry.xml",
                        new Metadata(),
                        links);

        // two canonical articles survive; the malformed loc/href are dropped
        assertEquals(2, result.numCanonicalLinks);
        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/news/good-before-bad.html"));
        assertTrue(emitted.contains("http://www.example.com/news/good-after-bad.html"));
        // the valid FR alternate of the second article is still emitted
        assertTrue(emitted.contains("http://www.example.com/fr/news/good-after-bad.html"));
    }

    @Test
    public void totallyBrokenContent_throwsInsteadOfSilentEmptySitemap() throws Exception {
        try {
            parseFixture(
                    "https://example.org/sitemap-totally-broken.xml",
                    "sitemap-totally-broken.xml",
                    new Metadata());
            fail("Expected an UnknownFormatException for unparseable content");
        } catch (UnknownFormatException expected) {
            // expected: the bolt routes this to Status.ERROR
            assertNotNull(expected);
        }
    }

    // ------------------------------------------------------------------
    // sitemap index: recency, de-duplication and partial failures
    // ------------------------------------------------------------------

    @Test
    public void sitemapIndex_mixedNewAndOld_onlyRecentSubSitemapsCount() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemapindex-mixed-new-old.xml",
                        "sitemapindex-mixed-new-old.xml",
                        new Metadata(),
                        links);

        assertEquals(2, result.numCanonicalLinks);
        assertEquals(1, result.numSkippedNotRecent);
        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/sitemaps/news-recent-1.xml"));
        assertTrue(emitted.contains("http://www.example.com/sitemaps/news-recent-2.xml"));
        assertFalse(emitted.contains("http://www.example.com/sitemaps/news-stale.xml"));
    }

    @Test
    public void sitemapIndex_duplicateSubSitemapEmittedOnce() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemapindex-duplicate-loc.xml",
                        "sitemapindex-duplicate-loc.xml",
                        new Metadata(),
                        links);

        assertEquals(2, result.numCanonicalLinks);
        assertEquals(1, result.numDuplicates);
        Set<String> emitted = targets(links);
        assertEquals(2, emitted.size());
        assertTrue(emitted.contains("http://www.example.com/sitemaps/news-a.xml"));
        assertTrue(emitted.contains("http://www.example.com/sitemaps/news-b.xml"));
    }

    @Test
    public void sitemapIndex_partialBrokenXml_restOfBatchSurvives() throws Exception {
        List<Outlink> links = new ArrayList<>();
        SiteMapParseResult result =
                parseInto(
                        "https://example.org/sitemapindex-partial-broken.xml",
                        "sitemapindex-partial-broken.xml",
                        new Metadata(),
                        links);

        assertEquals(2, result.numCanonicalLinks);
        Set<String> emitted = targets(links);
        assertTrue(emitted.contains("http://www.example.com/sitemaps/good-1.xml"));
        assertTrue(emitted.contains("http://www.example.com/sitemaps/good-2.xml"));
    }

    // ------------------------------------------------------------------
    // recursive index metadata inheritance: news type, verified, delay
    // ------------------------------------------------------------------

    @Test
    public void sitemapIndex_inheritsNewsTypeAndVerifiedFlag() throws Exception {
        List<Outlink> links = new ArrayList<>();
        Metadata parent = new Metadata();
        parent.setValue(NewsSiteMapParserBolt.isSitemapNewsKey, "true");
        parent.setValue(NewsSiteMapParserBolt.isSitemapVerifiedKey, "true");

        parseInto(
                "https://example.org/sitemapindex-mixed-new-old.xml",
                "sitemapindex-mixed-new-old.xml",
                parent,
                links);

        for (Outlink ol : links) {
            assertEquals(
                    "news type inherited",
                    "true",
                    ol.getMetadata().getFirstValue(NewsSiteMapParserBolt.isSitemapNewsKey));
            assertEquals(
                    "verified flag inherited",
                    "true",
                    ol.getMetadata().getFirstValue(NewsSiteMapParserBolt.isSitemapVerifiedKey));
            assertEquals("marked as sitemap", "true", ol.getMetadata().getFirstValue("isSitemap"));
        }
    }

    @Test
    public void sitemapIndex_nonNewsParentDoesNotMarkChildrenAsNews() throws Exception {
        List<Outlink> links = new ArrayList<>();
        parseInto(
                "https://example.org/sitemapindex-mixed-new-old.xml",
                "sitemapindex-mixed-new-old.xml",
                new Metadata(),
                links);

        for (Outlink ol : links) {
            assertEquals(
                    "false",
                    ol.getMetadata().getFirstValue(NewsSiteMapParserBolt.isSitemapNewsKey));
        }
    }

    @Test
    public void sitemapIndex_inheritsStaggeredSchedulingDelay() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("sitemap.sniffContent", true);
        config.put("sitemap.filter.hours.since.modified", 168);
        config.put("sitemap.schedule.delay", 15);
        prepareParserBolt("test.parsefilters.json", config);

        List<Outlink> links = new ArrayList<>();
        parseInto(
                "https://example.org/sitemapindex-mixed-new-old.xml",
                "sitemapindex-mixed-new-old.xml",
                new Metadata(),
                links);

        // two recent sub-sitemaps: first has no delay, second is delayed by 15 min
        Map<String, String> delayByTarget = new HashMap<>();
        for (Outlink ol : links) {
            delayByTarget.put(
                    ol.getTargetURL(),
                    ol.getMetadata().getFirstValue(DefaultScheduler.DELAY_METADATA));
        }
        assertEquals(2, delayByTarget.size());
        List<String> delays = new ArrayList<>(delayByTarget.values());
        assertTrue(delays.contains(null));
        assertTrue(delays.contains("15"));
    }

    // ------------------------------------------------------------------
    // bolt-level tuple/status output and numLinks metadata
    // ------------------------------------------------------------------

    @Test
    public void execute_emitsNumLinksAndStatusTuples() throws Exception {
        String url = "https://example.org/sitemap-news-multilingual.xml";
        Metadata metadata = new Metadata();
        super.parse(url, fixture("sitemap-news-multilingual.xml"), metadata);

        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // 4 article outlinks (DISCOVERED) + the sitemap itself (FETCHED)
        assertEquals(5, statusTuples.size());

        List<Object> fetched =
                statusTuples.stream()
                        .filter(t -> t.get(2) == Status.FETCHED)
                        .findFirst()
                        .orElseGet(
                                () -> {
                                    fail("expected a FETCHED tuple for the sitemap itself");
                                    return null;
                                });
        Metadata fetchedMetadata = (Metadata) fetched.get(1);
        assertEquals("2", fetchedMetadata.getFirstValue(NewsSiteMapParserBolt.numLinksKey));

        long discovered = statusTuples.stream().filter(t -> t.get(2) == Status.DISCOVERED).count();
        assertEquals(4, discovered);
        assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    public void execute_brokenSitemapEmitsErrorAndDropsStaleNumLinks() throws Exception {
        String url = "https://example.org/sitemap-totally-broken.xml";
        Metadata metadata = new Metadata();
        // bolt only processes tuples already classified as a news sitemap
        metadata.setValue(NewsSiteMapParserBolt.isSitemapNewsKey, "true");
        // pretend a previous successful parse had recorded links
        metadata.setValue(NewsSiteMapParserBolt.numLinksKey, "9");
        super.parse(url, fixture("sitemap-totally-broken.xml"), metadata);

        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        assertEquals(1, statusTuples.size());
        assertEquals(Status.ERROR, statusTuples.get(0).get(2));
        Metadata errorMetadata = (Metadata) statusTuples.get(0).get(1);
        assertEquals("sitemap parsing", errorMetadata.getFirstValue(Constants.STATUS_ERROR_SOURCE));
        assertFalse(errorMetadata.containsKey(NewsSiteMapParserBolt.numLinksKey));
        assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    public void execute_indexChildrenInheritNewsTypeOnStatusStream() throws Exception {
        String url = "https://example.org/sitemapindex-mixed-new-old.xml";
        Metadata metadata = new Metadata();
        metadata.setValue(NewsSiteMapParserBolt.isSitemapIndexKey, "true");
        metadata.setValue(NewsSiteMapParserBolt.isSitemapNewsKey, "true");
        // the parser reads the parent's news flag from metadata
        super.parse(url, fixture("sitemapindex-mixed-new-old.xml"), metadata);

        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        List<List<Object>> discovered =
                statusTuples.stream()
                        .filter(t -> t.get(2) == Status.DISCOVERED)
                        .collect(Collectors.toList());
        assertEquals(2, discovered.size());
        for (List<Object> tuple : discovered) {
            Metadata child = (Metadata) tuple.get(1);
            assertEquals("true", child.getFirstValue(NewsSiteMapParserBolt.isSitemapNewsKey));
            assertEquals("true", child.getFirstValue("isSitemap"));
        }
        List<Object> fetched =
                statusTuples.stream().filter(t -> t.get(2) == Status.FETCHED).findFirst().get();
        assertEquals(
                "2", ((Metadata) fetched.get(1)).getFirstValue(NewsSiteMapParserBolt.numLinksKey));
    }

    @Test
    public void execute_staleSitemapReportsZeroNumLinksForScheduler() throws Exception {
        // end-to-end path feeding NewsSitemapScheduler's tombstone predicate
        String url = "https://example.org/sitemap-news-mixed-new-old.xml";
        Metadata metadata = new Metadata();
        // bolt only processes tuples already classified as a news sitemap
        metadata.setValue(NewsSiteMapParserBolt.isSitemapNewsKey, "true");
        Map<String, Object> config = new HashMap<>();
        config.put("sitemap.sniffContent", true);
        config.put("sitemap.filter.hours.since.modified", 0);
        prepareParserBolt("test.parsefilters.json", config);
        super.parse(url, fixture("sitemap-news-mixed-new-old.xml"), metadata);

        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        List<Object> fetched =
                statusTuples.stream().filter(t -> t.get(2) == Status.FETCHED).findFirst().get();
        Metadata fetchedMetadata = (Metadata) fetched.get(1);
        assertEquals("0", fetchedMetadata.getFirstValue(NewsSiteMapParserBolt.numLinksKey));
        // the scheduler shares the same numLinks value: zero recent news makes
        // the sitemap eligible for the staleness tombstone once it stops changing
        fetchedMetadata.setValue("isSitemap", "true");
        fetchedMetadata.setValue(
                org.apache.stormcrawler.persistence.AdaptiveScheduler.SIGNATURE_MODIFIED_KEY,
                java.time.Instant.now().minusSeconds(3600).toString());
        assertTrue(
                NewsSitemapScheduler.isDeadEphemeralSitemap(
                        Status.FETCHED, fetchedMetadata, 60_000L));
    }

    @Test
    public void parseSiteMap_legacyMethodStillReturnsParsedDocument() throws Exception {
        // the protected legacy entry point must keep working for callers
        List<Outlink> links = new ArrayList<>();
        crawlercommons.sitemaps.AbstractSiteMap sm =
                ((NewsSiteMapParserBolt) bolt)
                        .parseSiteMap(
                                "https://example.org/sitemap-news-multilingual.xml",
                                fixture("sitemap-news-multilingual.xml"),
                                "",
                                new Metadata(),
                                links);
        assertNotNull(sm);
        assertFalse(sm.isIndex());
        // merger EN/DE/FR + standalone election article = 4 distinct URLs
        assertEquals(4, links.size());
    }
}
