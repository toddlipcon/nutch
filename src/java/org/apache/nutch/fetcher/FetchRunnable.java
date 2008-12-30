/**
 * Copyright 2008 The Apache Software Foundation
 *
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

package org.apache.nutch.fetcher;

import java.net.MalformedURLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.URLUtil;

/**
 * A Runnable that fetches a single FetchItem.
 */
public class FetchRunnable implements Runnable {
  public static final Log LOG = LogFactory.getLog(FetchRunnable.class);

  private final FetchItem fit;
  private final FetchSink _sink;
  private final ProtocolFactory _protocolFactory;

  private final int _maxRedirects;
  private final int _maxCrawlDelay;

  private final URLNormalizers _normalizers;
  private final URLFilters _urlFilters;


  public FetchRunnable(FetchItem fit, Configuration conf, FetchSink mapper) {
    this.fit = fit;
    _sink = mapper;
    _maxRedirects = FetcherConf.getMaxRedirects(conf);
    _maxCrawlDelay = FetcherConf.getMaxCrawlDelay(conf);
    _protocolFactory = new ProtocolFactory(conf);
    _normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
    _urlFilters = new URLFilters(conf);
  }

  private ProtocolOutput fetchProtocolOutput() throws ProtocolNotFound {
    Protocol protocol = _protocolFactory.getProtocol(fit.url.toString());
    RobotRules rules = protocol.getRobotRules(fit.url, fit.datum);

    LOG.info("Robot rules: " + String.valueOf(rules));

    // Check that we aren't denied by robot rules
    if (!rules.isAllowed(fit.getJavaUrl())) {
      LOG.debug("Denied by robots.txt: " + fit.url);
      _sink.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
      return null;
    }

    // Update crawl delay based on robot rules
    if (rules.getCrawlDelay() > _maxCrawlDelay) {
      LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
      _sink.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
      return null;
    }

    if (rules.getCrawlDelay() > 0) {
      _sink.setCrawlDelay(fit, rules.getCrawlDelay());
    }

    return protocol.getProtocolOutput(fit.url, fit.datum);

  }

  private FetchItem getRedirectedFetchItem(
    FetchItem origFit,
    String newUrl,
    boolean temporaryRedirect)
    throws MalformedURLException, URLFilterException
  {
    newUrl = filterAndNormalize(newUrl);

    // Redirect was filtered or was a self-redirect
    if (newUrl == null || newUrl.equals(origFit.getUrlString())) {
      LOG.debug(" - redirect skipped: " +
                (newUrl != null ? "to same url" : "filtered"));
      return null;
    }

    CrawlDatum newDatum = new CrawlDatum(
      CrawlDatum.STATUS_DB_UNFETCHED,
      fit.datum.getFetchInterval(), fit.datum.getScore());

    // The "representative URL" based on whether this redirect
    // is temporary.
    //
    // We start with either the original URL or the URL that it is
    // representive of (stored from previous redirect)
    //
    // This allows the representative URL to chain through a sequence
    // of rediects.
    String reprUrl = getReprUrl(origFit.getUrl(), origFit.getDatum());

    // Use the Yahoo Slurp algorithm to decide which URL is the true URL
    // of the page.
    reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temporaryRedirect);

    // If we have this, add it to the new metadata
    if (reprUrl != null) {
      newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                                 new Text(reprUrl));
    }

    return new FetchItem(new Text(newUrl), newDatum, origFit.getRedirectDepth() + 1);
  }

  private void handleRedirect(FetchItem origFit, String newUrl, boolean temporary)
    throws MalformedURLException, URLFilterException
  {
    FetchItem redirFit = getRedirectedFetchItem(origFit, newUrl, temporary);
    if (redirFit == null) {
      // bad redirect (filtered or circular)
      return;
    }

    if (_maxRedirects > 0 &&
        redirFit.getRedirectDepth() <= _maxRedirects) {
      // We have redirecting enabled, so add it to the queue
      _sink.submitFetchItem(redirFit);
    } else if (_maxRedirects > 0) {
      LOG.info(" - redirect count exceeded " + fit.url);
      _sink.output(origFit.url, origFit.datum, null,
                     ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatum.STATUS_FETCH_GONE);
    }


  }

  public void run() {
    try {
      LOG.info("fetching " + fit.url);
      LOG.debug("redirectCount=" + fit.getRedirectDepth());

      // Actually fetch the item
      ProtocolOutput output = fetchProtocolOutput();

      // No output is possible if the host is down, denied by robots, etc
      if (output == null)
        return;

      ProtocolStatus status = output.getStatus();
      Content content = output.getContent();

      switch(status.getCode()) {                
        case ProtocolStatus.WOULDBLOCK:
          _sink.submitFetchItem(fit); // retry
          return;

        case ProtocolStatus.SUCCESS:        // got a page
          ParseStatus parseStatus =
            _sink.output(fit.url, fit.datum, content, status, CrawlDatum.STATUS_FETCH_SUCCESS);
          _sink.updateStatus(content.getContent().length);

          // Check for a redirect in the actual parsed content
          if (parseStatus != null && parseStatus.isSuccess() &&
              parseStatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {

            int refreshTime = Integer.valueOf(parseStatus.getArgs()[1]);
            boolean temporary = refreshTime < Fetcher.PERM_REFRESH_TIME;

            handleRedirect(fit, parseStatus.getMessage(), temporary);
          }
          break;

        case ProtocolStatus.MOVED:         // redirect by protocol
        case ProtocolStatus.TEMP_MOVED:
          int code;
          boolean temporary;
          if (status.getCode() == ProtocolStatus.MOVED) {
            code = CrawlDatum.STATUS_FETCH_REDIR_PERM;
            temporary = false;
          } else {
            code = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
            temporary = true;
          }
          _sink.output(fit.url, fit.datum, content, status, code);
          handleRedirect(fit, status.getMessage(), temporary);
          break;

        case ProtocolStatus.EXCEPTION:
          _sink.logError(fit.url, status.getMessage());
          /* FALLTHROUGH */
        case ProtocolStatus.RETRY:          // retry
        case ProtocolStatus.BLOCKED:
          _sink.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
          break;
                
        case ProtocolStatus.GONE:           // gone
        case ProtocolStatus.NOTFOUND:
        case ProtocolStatus.ACCESS_DENIED:
        case ProtocolStatus.ROBOTS_DENIED:
          _sink.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
          break;

        case ProtocolStatus.NOTMODIFIED:
          _sink.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_NOTMODIFIED);
          break;

        default:
          LOG.warn("Unknown ProtocolStatus: " + status.getCode());
          _sink.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
      }
    } catch (Throwable t) {
      _sink.logError(fit.url, StringUtils.stringifyException(t));
      _sink.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED, CrawlDatum.STATUS_FETCH_RETRY);
    }
  }

  /**
   * Given the key and value for a Mapper input, determine
   * the actual URL we want to fetch.
   *
   * @return url
   */
  private String getReprUrl(Text key, CrawlDatum datum) {
    Text reprUrlWritable =
      (Text) datum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
    if (reprUrlWritable == null) {
      return key.toString();
    } else {
      return reprUrlWritable.toString();
    }
  }

  /**
   * Filter and normalize the given url.
   *
   * @return the normalized URL, or null if it should be filtered.
   */
  private String filterAndNormalize(String url) 
    throws MalformedURLException, URLFilterException
  {
    String newUrl = _normalizers.normalize(url, URLNormalizers.SCOPE_FETCHER);
    return _urlFilters.filter(newUrl);
  }


}
