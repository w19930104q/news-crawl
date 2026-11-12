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

import static org.apache.stormcrawler.Constants.StatusStreamName;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.Namespace;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;
import crawlercommons.sitemaps.UnknownFormatException;
import crawlercommons.sitemaps.extension.Extension;
import crawlercommons.sitemaps.extension.ExtensionMetadata;
import crawlercommons.sitemaps.extension.LinkAttributes;
import crawlercommons.sitemaps.extension.NewsAttributes;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseFilters;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.persistence.DefaultScheduler;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/**
 * ParserBolt for <link href= "https://support.google.com/news/publisher/answer/74288?hl=en">news
 * sitemaps</a>.
 */
@SuppressWarnings("serial")
public class NewsSiteMapParserBolt extends SiteMapParserBolt {
    // TODO:
    // this is a modified copy of c.d.s.bolt.SiteMapParserBolt
    // - make parent class extensible and overridable
    // modifications:
    // - detect and process only Google news sitemaps
    // - or a sitemapindex because some subsitemaps may
    // be news sitemaps
    // - pass "isSitemapNews" to status metadata
    // - merge language-alternate pages (xhtml:link) and duplicate <loc>
    // entries into a single canonical article so that numLinks only counts
    // distinct new articles

    public static enum SitemapType {
        NEWS,
        INDEX,
        SITEMAP
    }

    public static final String isSitemapNewsKey = "isSitemapNews";
    public static final String isSitemapIndexKey = "isSitemapIndex";

    /**
     * A sitemap (not necessarily a news sitemap) which is verified to contain links to news
     * articles. Necessary to crawl news sites which provide a sitemap but neither a news feed or
     * sitemap.
     */
    public static final String isSitemapVerifiedKey = "isSitemapVerified";

    /** Number of (recent) links extracted from the sitemap on the last parse. */
    public static final String numLinksKey = "numLinks";

    private static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(NewsSiteMapParserBolt.class);

    /* content clues for news sitemaps, sitemap indexes or any sitemaps */
    public static String[][] contentClues;
    public static int contentCluesSitemapNewsMatchUpTo = -1;
    public static int contentCluesSitemapIndexMatchUpTo = -1;

    static {
        int cluesSize = Namespace.NEWS.length + 1 + 1 + Namespace.SITEMAP_LEGACY.length;
        contentClues = new String[cluesSize][1];
        int j = 0;
        // match 0-m: a news sitemap
        for (int i = 0; i < Namespace.NEWS.length; i++, j++) {
            contentClues[j][0] = Namespace.NEWS[i];
            contentCluesSitemapNewsMatchUpTo = j;
        }
        // m < match <= n: a sitemapindex
        contentClues[j][0] = "<sitemapindex";
        contentCluesSitemapIndexMatchUpTo = j;
        j++;
        // match > n: a "simple" sitemap
        contentClues[j][0] = Namespace.SITEMAP;
        j++;
        for (int i = 0; i < Namespace.SITEMAP_LEGACY.length; i++, j++) {
            contentClues[j][0] = Namespace.SITEMAP_LEGACY[i];
        }
    }

    private static ContentDetector contentDetector;
    private static ContentDetector rssContentDetector;

    private boolean strictModeSitemaps = false;
    private boolean allowPartialSitemaps = true;
    private boolean sniffContent = false;

    private ParseFilter parseFilters;
    private int filterHoursSinceModified = -1;

    private ReducedMetric averagedMetrics;

    /** Delay in minutes used for scheduling sub-sitemaps * */
    private int scheduleSitemapsWithDelay = -1;

    /**
     * Outcome of parsing a sitemap document: the parsed document plus the number of canonical
     * articles (or sub-sitemaps) which survived de-duplication and the recency filter. The count is
     * the single source of truth used both for the {@link #numLinksKey} metadata (which drives the
     * stale-sitemap decision in the scheduler) and for the parse-log counters.
     */
    public static class SiteMapParseResult {
        public final AbstractSiteMap siteMap;

        /** Distinct recent articles (urlset) or sub-sitemaps (index) emitted. */
        public final int numCanonicalLinks;

        /** Raw entries seen in the document before merging/filtering. */
        public final int numEntriesFound;

        /** Entries merged away as duplicate loc or language-alternate pages. */
        public final int numDuplicates;

        /** Entries skipped because their effective date is too old. */
        public final int numSkippedNotRecent;

        SiteMapParseResult(
                AbstractSiteMap siteMap,
                int numCanonicalLinks,
                int numEntriesFound,
                int numDuplicates,
                int numSkippedNotRecent) {
            this.siteMap = siteMap;
            this.numCanonicalLinks = numCanonicalLinks;
            this.numEntriesFound = numEntriesFound;
            this.numDuplicates = numDuplicates;
            this.numSkippedNotRecent = numSkippedNotRecent;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // TODO check that we have the right number of fields?
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");

        boolean isSitemap = Boolean.valueOf(metadata.getFirstValue(SiteMapParserBolt.isSitemapKey));
        boolean isNewsSitemap = Boolean.valueOf(metadata.getFirstValue(isSitemapNewsKey));
        boolean isSitemapIndex = Boolean.valueOf(metadata.getFirstValue(isSitemapIndexKey));
        boolean isSitemapVerified = Boolean.valueOf(metadata.getFirstValue(isSitemapVerifiedKey));

        // the news type must be inherited by sub-sitemaps even though sniffing
        // an index resets the news flag on the index document itself
        boolean inheritedNewsSitemap = isNewsSitemap;

        if (sniffContent) {
            SitemapType type = detectContent(url, content);
            if (type != null) {
                isSitemap = true;
                metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
                if (type == SitemapType.NEWS) {
                    isNewsSitemap = true;
                    inheritedNewsSitemap = true;
                    metadata.setValue(isSitemapNewsKey, "true");
                } else if (type == SitemapType.INDEX) {
                    isSitemapIndex = true;
                    if (isNewsSitemap) {
                        metadata.setValue(isSitemapNewsKey, "false");
                    }
                    isNewsSitemap = false;
                    metadata.setValue(isSitemapIndexKey, "true");
                } else if (isSitemapVerified) {
                    // do not reset metadata for verified sitemaps
                } else {
                    // sitemaps may change: reset wrong metadata values from
                    // previous fetches
                    if (isNewsSitemap) {
                        metadata.setValue(isSitemapNewsKey, "false");
                    }
                    if (isSitemapIndex) {
                        metadata.setValue(isSitemapIndexKey, "false");
                    }
                    isNewsSitemap = false;
                    isSitemapIndex = false;
                }
            }
        }

        if (isNewsSitemap || isSitemapIndex || isSitemapVerified) {
            /*
             * remove the isSitemap key from metadata to avoid that the default sitemap
             * fetch interval is applied to news sitemaps, sitemap indexes and verified
             * sitemaps
             */
            metadata.remove(isSitemapKey);
        } else {
            if (isSitemap) {
                collector.emit(
                        Constants.StatusStreamName,
                        tuple,
                        new Values(url, metadata, Status.FETCHED));
            } else {
                // not a sitemap, just pass it on
                collector.emit(tuple, tuple.getValues());
            }
            collector.ack(tuple);
            // skip everything which is not a news sitemap or a sitemap index
            return;
        }

        String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);

        AbstractSiteMap sitemap;
        List<Outlink> outlinks = new ArrayList<>();
        SiteMapParseResult parseResult;

        try {
            parseResult =
                    parseSiteMapWithResult(
                            url, content, ct, metadata, inheritedNewsSitemap, outlinks);
            sitemap = parseResult.siteMap;
        } catch (Exception e) {
            // exception while parsing the sitemap
            String errorMessage = "Exception while parsing " + url + ": " + e;
            LOG.error(errorMessage);
            // send to status stream in case another component wants to update
            // its status
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "sitemap parsing");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            metadata.remove(numLinksKey);
            collector.emit(
                    Constants.StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
            collector.ack(tuple);
            return;
        }

        // apply the parse filters if any to the current document
        try {
            ParseResult parse = new ParseResult();
            parse.setOutlinks(outlinks);
            ParseData parseData = parse.get(url);
            parseData.setMetadata(metadata);

            parseFilters.filter(url, content, null, parse);
        } catch (RuntimeException e) {
            String errorMessage = "Exception while running parse filters on " + url + ": " + e;
            LOG.error(errorMessage);
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "content filtering");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            metadata.remove(numLinksKey);
            collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
            collector.ack(tuple);
            return;
        }

        // check whether parsed sitemap is index
        if (sitemap.isIndex()) {
            isSitemapIndex = true;
            metadata.setValue(isSitemapIndexKey, "true");
        } else {
            isSitemapIndex = false;
            metadata.remove(isSitemapIndexKey);
        }

        // send outlinks to status stream. The sitemap/index metadata
        // (isSitemap, inherited news type, verified flag, scheduling delay) is
        // already attached by parseSiteMapIndex
        for (Outlink ol : outlinks) {
            Values v = new Values(ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED);
            collector.emit(Constants.StatusStreamName, tuple, v);
        }

        // track the number of distinct recent articles/sub-sitemaps found:
        // language alternates and duplicate locs are merged into one canonical
        // article, so an unchanged sitemap without new articles reports 0
        metadata.setValue(numLinksKey, String.valueOf(parseResult.numCanonicalLinks));

        // marking the main URL as successfully fetched
        collector.emit(
                Constants.StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
        collector.ack(tuple);
    }

    public SitemapType detectContent(String url, byte[] content) {
        // try to detect content based on the first n bytes
        // works for XML and non-compressed documents
        // TODO: implement check for compressed XML
        // First check if it's an RSS or atom feed - if so, return null
        if (rssContentDetector.getFirstMatch(content) >= 0) {
            LOG.info("{} detected as feed - skipping", url);
            return null;
        }

        int match = contentDetector.getFirstMatch(content);
        if (match >= 0) {
            // a sitemap, need to detect type of sitemap
            if (match <= contentCluesSitemapNewsMatchUpTo) {
                LOG.info("{} detected as news sitemap based on content", url);
                return SitemapType.NEWS;
            } else if (match <= contentCluesSitemapIndexMatchUpTo) {
                LOG.info("{} detected as sitemap index based on content", url);
                return SitemapType.INDEX;
            } else {
                return SitemapType.SITEMAP;
            }
        }
        return null;
    }

    private boolean recentlyModified(Date lastModified) {
        if (lastModified != null && filterHoursSinceModified != -1) {
            // filter based on the published date
            Calendar rightNow = Calendar.getInstance();
            rightNow.add(Calendar.HOUR, -filterHoursSinceModified);
            if (lastModified.before(rightNow.getTime())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Backward-compatible parse entry: parses the sitemap and appends outlinks to {@code links}.
     * Prefer {@link #parseSiteMapWithResult} to also get the canonical-link counts.
     */
    protected AbstractSiteMap parseSiteMap(
            String url,
            byte[] content,
            String contentType,
            Metadata parentMetadata,
            List<Outlink> links)
            throws UnknownFormatException, IOException {
        boolean inheritedNews =
                Boolean.parseBoolean(parentMetadata.getFirstValue(isSitemapNewsKey));
        return parseSiteMapWithResult(
                        url, content, contentType, parentMetadata, inheritedNews, links)
                .siteMap;
    }

    protected SiteMapParseResult parseSiteMapWithResult(
            String url,
            byte[] content,
            String contentType,
            Metadata parentMetadata,
            List<Outlink> links)
            throws UnknownFormatException, IOException {
        boolean inheritedNews =
                Boolean.parseBoolean(parentMetadata.getFirstValue(isSitemapNewsKey));
        return parseSiteMapWithResult(
                url, content, contentType, parentMetadata, inheritedNews, links);
    }

    /**
     * Parses a sitemap document.
     *
     * @param inheritedNewsSitemap whether the document was marked (or sniffed) as a news sitemap on
     *     the way in; the flag is inherited by sub-sitemaps of an index even when the index
     *     document itself is not a news urlset
     */
    protected SiteMapParseResult parseSiteMapWithResult(
            String url,
            byte[] content,
            String contentType,
            Metadata parentMetadata,
            boolean inheritedNewsSitemap,
            List<Outlink> links)
            throws UnknownFormatException, IOException {

        SiteMapParser parser = new SiteMapParser(strictModeSitemaps, allowPartialSitemaps);
        parser.setStrictNamespace(true);
        parser.addAcceptedNamespace(Namespace.SITEMAP_LEGACY);
        parser.addAcceptedNamespace(Namespace.EMPTY);
        // enable extensions (also adds extension namespaces)
        parser.enableExtension(Extension.NEWS);
        parser.enableExtension(Extension.LINKS);

        URL sURL = new URL(url);
        long start = System.currentTimeMillis();
        AbstractSiteMap siteMap;
        // let the parser guess what the mimetype is
        if (StringUtils.isBlank(contentType) || contentType.contains("octet-stream")) {
            siteMap = parser.parseSiteMap(content, sURL);
        } else {
            siteMap = parser.parseSiteMap(contentType, content, sURL);
        }
        long end = System.currentTimeMillis();
        averagedMetrics.update(end - start);

        final SiteMapParseResult result;
        if (siteMap.isIndex()) {
            result =
                    parseSitemapIndex(
                            sURL,
                            (SiteMapIndex) siteMap,
                            parentMetadata,
                            inheritedNewsSitemap,
                            links);
        } else {
            result = parseSitemapUrls(sURL, (SiteMap) siteMap, parentMetadata, links);
        }
        return result;
    }

    /**
     * Parses a sitemap index. Sub-sitemaps inherit the news-sitemap type and verified state of the
     * parent document and keep the staggered scheduling delay. A single malformed sub-sitemap entry
     * is skipped without losing the remaining batch; duplicate sub-sitemap URLs are emitted only
     * once.
     */
    private SiteMapParseResult parseSitemapIndex(
            URL sURL,
            SiteMapIndex smi,
            Metadata parentMetadata,
            boolean inheritedNewsSitemap,
            List<Outlink> links) {
        Collection<AbstractSiteMap> subsitemaps = smi.getSitemaps();
        int linksFound = 0;
        int linksSkippedNotRecentlyModified = 0;
        int duplicates = 0;
        int canonical = 0;
        int delay = 0;

        boolean parentVerified =
                Boolean.parseBoolean(parentMetadata.getFirstValue(isSitemapVerifiedKey));

        Set<String> emitted = new HashSet<>();
        Iterator<AbstractSiteMap> iter = subsitemaps.iterator();
        while (iter.hasNext()) {
            linksFound++;
            AbstractSiteMap asm = iter.next();
            String target;
            Date lastModified;
            try {
                target = asm.getUrl().toExternalForm();
                lastModified = asm.getLastModified();
            } catch (RuntimeException e) {
                // a single broken sub-sitemap entry must not drop the whole
                // index batch
                LOG.warn("Skipping malformed sub-sitemap entry in {}: {}", sURL, e.toString());
                continue;
            }

            if (!recentlyModified(lastModified)) {
                linksSkippedNotRecentlyModified++;
                LOG.debug(
                        "{} has a modified date {} which is more than {} hours old",
                        target,
                        lastModified,
                        filterHoursSinceModified);
                continue;
            }

            Outlink ol =
                    filterOutlink(
                            sURL,
                            target,
                            parentMetadata,
                            isSitemapKey,
                            "true",
                            isSitemapNewsKey,
                            Boolean.toString(inheritedNewsSitemap));
            if (ol == null) {
                // filtered by URL filters (e.g. malformed URL)
                continue;
            }
            if (!emitted.add(ol.getTargetURL())) {
                duplicates++;
                LOG.debug("Duplicate sub-sitemap skipped in {}: {}", sURL, target);
                continue;
            }

            if (inheritedNewsSitemap) {
                // inherit the news-sitemap type: a sub-sitemap of a news
                // sitemap index is expected to hold news articles
                ol.getMetadata().setValue(isSitemapNewsKey, "true");
            }
            if (parentVerified) {
                // mark sitemaps from a verified sitemap index also as "verified"
                ol.getMetadata().setValue(isSitemapVerifiedKey, "true");
            }

            // add a delay
            if (this.scheduleSitemapsWithDelay > 0) {
                if (delay > 0) {
                    ol.getMetadata()
                            .setValue(DefaultScheduler.DELAY_METADATA, Integer.toString(delay));
                }
                delay += this.scheduleSitemapsWithDelay;
            }

            links.add(ol);
            canonical++;
            LOG.debug("{} : [sitemap] {}", sURL, target);
        }
        LOG.info(
                "Sitemap index (found {} sitemaps, {} duplicates, {} skipped): {}",
                linksFound,
                duplicates,
                linksSkippedNotRecentlyModified,
                sURL);
        return new SiteMapParseResult(
                smi, canonical, linksFound, duplicates, linksSkippedNotRecentlyModified);
    }

    /**
     * Parses a urlset. All {@code <loc>} entries and their {@code xhtml:link} language-alternate
     * pages are first merged into canonical articles (connected components; duplicate locs collapse
     * into one). For every canonical article the effective publication date is the newest valid
     * {@code news:publication_date} in the group, falling back to the newest valid {@code lastmod}
     * when no valid publication date exists. Only articles passing the recency filter are emitted,
     * and only those count towards {@code numLinks}.
     */
    private SiteMapParseResult parseSitemapUrls(
            URL sURL, SiteMap sm, Metadata parentMetadata, List<Outlink> links) {
        Collection<SiteMapURL> sitemapURLs = sm.getSiteMapUrls();

        // first pass: build the canonical groups (loc + xhtml alternates as
        // connected components) and merge the dates carried by each entry
        DisjointSet<String> components = new DisjointSet<>();
        Map<String, ArticleGroup> groups = new LinkedHashMap<>();
        Set<String> seenTargets = new HashSet<>();
        int duplicateLocEntries = 0;
        int linksFound = 0;

        for (SiteMapURL smurl : sitemapURLs) {
            linksFound++;
            String target;
            try {
                target = smurl.getUrl().toExternalForm();
            } catch (RuntimeException e) {
                LOG.warn("Skipping malformed sitemap URL entry in {}: {}", sURL, e.toString());
                continue;
            }
            components.add(target);
            if (!seenTargets.add(target)) {
                // another <url> entry with the same loc: same logical article
                duplicateLocEntries++;
            }
            ArticleGroup group = groups.computeIfAbsent(target, ArticleGroup::new);
            group.merge(smurl);

            ExtensionMetadata[] linkAttrs = smurl.getAttributesForExtension(Extension.LINKS);
            if (linkAttrs != null) {
                for (ExtensionMetadata attr : linkAttrs) {
                    LinkAttributes linkAttr = (LinkAttributes) attr;
                    URL href = linkAttr.getHref();
                    if (href == null) {
                        continue;
                    }
                    String hrefStr = href.toString();
                    components.add(hrefStr);
                    components.union(target, hrefStr);
                    // an alternate page may only appear as xhtml:link and not
                    // as its own <loc>: make sure it gets a group as well
                    groups.computeIfAbsent(hrefStr, ArticleGroup::new);
                }
            }
        }

        // collapse the per-loc groups by their connected component
        Map<String, ArticleGroup> canonical = new LinkedHashMap<>();
        int mergedGroups = 0;
        for (ArticleGroup group : groups.values()) {
            String root = components.find(group.canonicalUrl);
            ArticleGroup merged = canonical.get(root);
            if (merged == null) {
                canonical.put(root, group);
            } else {
                // duplicate loc or an xhtml:link-connected entry of the same
                // logical article: merge, do not count as another article
                merged.absorb(group);
                mergedGroups++;
            }
        }

        // second pass: one recency decision per canonical article, then emit
        // the canonical loc and its language alternates once each
        int linksSkippedNotRecentlyModified = 0;
        int duplicates = mergedGroups + duplicateLocEntries;
        int numCanonical = 0;
        Set<String> emitted = new HashSet<>();

        for (ArticleGroup group : canonical.values()) {
            Date effectiveDate = group.effectiveDate();
            if (!recentlyModified(effectiveDate)) {
                linksSkippedNotRecentlyModified++;
                LOG.debug(
                        "{} canonical article {} dated {} is more than {} hours old",
                        sURL,
                        group.canonicalUrl,
                        effectiveDate,
                        filterHoursSinceModified);
                continue;
            }

            Outlink canonicalOutlink =
                    filterOutlink(
                            sURL,
                            group.canonicalUrl,
                            parentMetadata,
                            isSitemapKey,
                            "false",
                            isSitemapNewsKey,
                            "false");
            if (canonicalOutlink == null) {
                continue;
            }
            if (emitted.add(canonicalOutlink.getTargetURL())) {
                links.add(canonicalOutlink);
                numCanonical++;
            } else {
                duplicates++;
            }

            // emit the language-alternate pages of this article, each once
            for (String alternate : group.alternates) {
                if (alternate.equals(group.canonicalUrl)) {
                    // skip href links duplicating the sitemap URL
                    continue;
                }
                Outlink ol =
                        filterOutlink(
                                sURL,
                                alternate,
                                parentMetadata,
                                isSitemapKey,
                                "false",
                                isSitemapNewsKey,
                                "false");
                if (ol == null) {
                    continue;
                }
                if (emitted.add(ol.getTargetURL())) {
                    links.add(ol);
                } else {
                    duplicates++;
                }
            }
        }
        LOG.info(
                "Sitemap (found {} article entries, {} canonical recent articles, {} URLs merged as"
                        + " duplicates/alternates, {} entries skipped as not recent): {}",
                linksFound,
                numCanonical,
                duplicates,
                linksSkippedNotRecentlyModified,
                sURL);
        return new SiteMapParseResult(
                sm, numCanonical, linksFound, duplicates, linksSkippedNotRecentlyModified);
    }

    /**
     * A canonical news article: the loc of its first {@code <url>} entry, the language-alternate
     * URLs pointing at the same article and the newest valid dates seen across all its entries.
     */
    private static final class ArticleGroup {
        private final String canonicalUrl;
        private final Set<String> alternates = new HashSet<>();
        private Date latestPublicationDate;
        private Date latestLastModified;

        ArticleGroup(String canonicalUrl) {
            this.canonicalUrl = canonicalUrl;
        }

        /** Merges the dates and alternate links carried by one {@code <url>} entry. */
        void merge(SiteMapURL smurl) {
            latestLastModified = maxDate(latestLastModified, smurl.getLastModified());
            ExtensionMetadata[] newsAttrs = smurl.getAttributesForExtension(Extension.NEWS);
            if (newsAttrs != null) {
                for (ExtensionMetadata attr : newsAttrs) {
                    NewsAttributes newsAttr = (NewsAttributes) attr;
                    // invalid publication dates parse to null and are ignored
                    latestPublicationDate =
                            maxDate(latestPublicationDate, newsAttr.getPublicationDate());
                }
            }
            ExtensionMetadata[] linkAttrs = smurl.getAttributesForExtension(Extension.LINKS);
            if (linkAttrs != null) {
                for (ExtensionMetadata attr : linkAttrs) {
                    LinkAttributes linkAttr = (LinkAttributes) attr;
                    URL href = linkAttr.getHref();
                    if (href != null) {
                        alternates.add(href.toString());
                    }
                }
            }
        }

        /** Merges another group representing the same canonical article. */
        void absorb(ArticleGroup other) {
            this.latestPublicationDate =
                    maxDate(this.latestPublicationDate, other.latestPublicationDate);
            this.latestLastModified = maxDate(this.latestLastModified, other.latestLastModified);
            this.alternates.addAll(other.alternates);
        }

        /**
         * Effective article date: the newest valid news publication date, or the newest valid
         * lastmod when no valid publication date is available.
         */
        Date effectiveDate() {
            if (latestPublicationDate != null) {
                return latestPublicationDate;
            }
            return latestLastModified;
        }

        private static Date maxDate(Date a, Date b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return a.after(b) ? a : b;
        }
    }

    /** Minimal union-find over strings to connect locs with their alternate pages. */
    private static final class DisjointSet<T> {
        private final Map<T, T> parent = new HashMap<>();

        void add(T item) {
            parent.putIfAbsent(item, item);
        }

        T find(T item) {
            T root = item;
            while (!root.equals(parent.get(root))) {
                root = parent.get(root);
            }
            // path compression
            T current = item;
            while (!current.equals(root)) {
                T next = parent.get(current);
                parent.put(current, root);
                current = next;
            }
            return root;
        }

        void union(T a, T b) {
            add(a);
            add(b);
            T rootA = find(a);
            T rootB = find(b);
            if (!rootA.equals(rootB)) {
                parent.put(rootB, rootA);
            }
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        sniffContent = ConfUtils.getBoolean(stormConf, "sitemap.sniffContent", false);
        filterHoursSinceModified =
                ConfUtils.getInt(stormConf, "sitemap.filter.hours.since.modified", -1);
        parseFilters = ParseFilters.fromConf(stormConf);
        int maxOffsetGuess = ConfUtils.getInt(stormConf, "sitemap.offset.guess", 1024);
        contentDetector = new ContentDetector(NewsSiteMapParserBolt.contentClues, maxOffsetGuess);
        rssContentDetector = new ContentDetector(FeedDetectorBolt.contentClues, maxOffsetGuess);
        averagedMetrics =
                context.registerMetric(
                        "news_sitemap_average_processing_time",
                        new ReducedMetric(new MeanReducer()),
                        30);
        scheduleSitemapsWithDelay =
                ConfUtils.getInt(stormConf, "sitemap.schedule.delay", scheduleSitemapsWithDelay);
    }
}
