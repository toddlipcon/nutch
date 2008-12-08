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

import java.io.*;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;


public class Fetcher extends Configured {

  public static final Log LOG = LogFactory.getLog(Fetcher.class);

  public static final int PERM_REFRESH_TIME = 5;

  public static final String CONTENT_REDIR = "content";

  public static final String PROTOCOL_REDIR = "protocol";


  public static class InputFormat extends SequenceFileInputFormat<Text, CrawlDatum> {
    /** Don't split inputs, to keep things polite. */
    public InputSplit[] getSplits(JobConf job, int nSplits)
      throws IOException {
      FileStatus[] files = listStatus(job);
      FileSplit[] splits = new FileSplit[files.length];
      FileSystem fs = FileSystem.get(job);
      for (int i = 0; i < files.length; i++) {
        FileStatus cur = files[i];
        splits[i] = new FileSplit(cur.getPath(), 0,
            cur.getLen(), (String[])null);
      }
      return splits;
    }
  }

  /**
   * A Runnable that fetches a single FetchItem.
   */
  private static class FetchRunnable implements Runnable {
    private final FetchItem fit;
    private final FetchMapper _mapper;

    private final int _maxRedirects;
    private final int _maxCrawlDelay;

    public FetchRunnable(FetchItem fit, FetchMapper mapper) {
      this.fit = fit;
      _mapper = mapper;
      _maxRedirects = mapper.getMaxRedirects();
      _maxCrawlDelay = _mapper.getMaxCrawlDelay();
    }

    private ProtocolOutput fetchProtocolOutput() throws ProtocolNotFound {
      Protocol protocol = _mapper._protocolFactory.getProtocol(fit.url.toString());
      RobotRules rules = protocol.getRobotRules(fit.url, fit.datum);

      // Check that we aren't denied by robot rules
      if (!rules.isAllowed(fit.getJavaUrl())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Denied by robots.txt: " + fit.url);
        }
        _mapper.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
        return null;
      }

      // Update crawl delay based on robot rules
      if (rules.getCrawlDelay() > _maxCrawlDelay) {
        LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
        _mapper.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
        return null;
      }

      if (rules.getCrawlDelay() > 0) {
        _mapper.setCrawlDelay(fit, rules.getCrawlDelay());
      }

      return protocol.getProtocolOutput(fit.url, fit.datum);

    }

    private FetchItem getRedirectedFetchItem(
      FetchItem origFit,
      String newUrl,
      boolean temporaryRedirect)
      throws MalformedURLException, URLFilterException
    {
      newUrl = _mapper.filterAndNormalize(newUrl);

      // Redirect was filtered or was a self-redirect
      if (newUrl == null || newUrl.equals(origFit.getUrlString())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - redirect skipped: " +
                    (newUrl != null ? "to same url" : "filtered"));
        }
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
        _mapper.submitFetchItem(redirFit);
      } else if (_maxRedirects > 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info(" - redirect count exceeded " + fit.url);
        }
        _mapper.output(origFit.url, origFit.datum, null,
               ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatum.STATUS_FETCH_GONE);
      }


    }

    public void run() {
      try {
        if (LOG.isInfoEnabled()) { LOG.info("fetching " + fit.url); }

        if (LOG.isDebugEnabled()) {
          LOG.debug("redirectCount=" + fit.getRedirectDepth());
        }

        // Actually fetch the item
        ProtocolOutput output = fetchProtocolOutput();
        ProtocolStatus status = output.getStatus();
        Content content = output.getContent();

        switch(status.getCode()) {                
          case ProtocolStatus.WOULDBLOCK:
            _mapper.submitFetchItem(fit); // retry
            return;

          case ProtocolStatus.SUCCESS:        // got a page
            ParseStatus parseStatus =
              _mapper.output(fit.url, fit.datum, content, status, CrawlDatum.STATUS_FETCH_SUCCESS);
            _mapper.updateStatus(content.getContent().length);

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
            _mapper.output(fit.url, fit.datum, content, status, code);
            handleRedirect(fit, status.getMessage(), temporary);
            break;

          case ProtocolStatus.EXCEPTION:
            _mapper.logError(fit.url, status.getMessage());
            /* FALLTHROUGH */
          case ProtocolStatus.RETRY:          // retry
          case ProtocolStatus.BLOCKED:
            _mapper.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
            break;
                
          case ProtocolStatus.GONE:           // gone
          case ProtocolStatus.NOTFOUND:
          case ProtocolStatus.ACCESS_DENIED:
          case ProtocolStatus.ROBOTS_DENIED:
            _mapper.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
            break;

          case ProtocolStatus.NOTMODIFIED:
            _mapper.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_NOTMODIFIED);
            break;

          default:
            if (LOG.isWarnEnabled()) {
              LOG.warn("Unknown ProtocolStatus: " + status.getCode());
            }
            _mapper.output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
        }
      } catch (Throwable t) {
        _mapper.logError(fit.url, t.toString());
        _mapper.output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED, CrawlDatum.STATUS_FETCH_RETRY);
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

  }

  /**
   * The Mapper that runs over a single part file inside a segment, fetching
   * each of the CrawlDatums and outputting CrawlDatums for the fetched responses.
   */
  private static class FetchMapper extends Configured
    implements MapRunnable<Text, CrawlDatum, Text, NutchWritable>
  {
    private int _maxRedirects;
    private int _maxCrawlDelay;
    private int _threadCount;
    private boolean _isParsing;
    private boolean _isStoringContent;
    private String _segmentName;

    private ExecutorService _executor;
    private FetchQueue _fetchQueue;
    private QueuePartitioner _queuePartitioner;

    private OutputCollector<Text, NutchWritable> _output;

    private ParseUtil _parseUtil;
    private ScoringFilters _scFilters;
    private URLFilters _urlFilters;
    private URLNormalizers _normalizers;
    private ProtocolFactory _protocolFactory;

    public static enum Counters {
      QUEUES_FULL_WAIT
    };

    public void configure(JobConf conf) {
      setConf(conf);

      _protocolFactory = new ProtocolFactory(conf);
      _normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
      _urlFilters = new URLFilters(conf);
      _scFilters = new ScoringFilters(conf);
      _parseUtil = new ParseUtil(conf);

      _maxRedirects = conf.getInt("http.redirect.max", 3);
      _maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
      _isParsing = FetcherConf.isParsing(conf);
      _isStoringContent = FetcherConf.isStoringContent(conf);

      _segmentName = conf.get(Nutch.SEGMENT_NAME_KEY);
      _threadCount = conf.getInt("fetcher.threads.fetch", 10);

      if (LOG.isInfoEnabled()) { LOG.info("Fetcher: threads: " + _threadCount); }

      _executor = startExecutor();
      _fetchQueue = new FetchQueue(
        _executor,
        conf.getInt("fetcher.threads.per.host", 1),
        (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000));

      _queuePartitioner = createQueuePartitioner(conf);
    }

    /**
     * Create a QueuePartitioner based on the configured queueing policy.
     */
    private QueuePartitioner createQueuePartitioner(JobConf conf) {
      boolean byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", false);

      if (byIP)
        return new ByIpQueuePartitioner();
      else
        return new ByHostnameQueuePartitioner();
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

    /**
     * Start the ExecutorService on which the actual fetch jobs will run.
     */
    private ExecutorService startExecutor() {
      return Executors.newFixedThreadPool(_threadCount);
    }

    /**
     * Submit a FetchItem to its appropriate queue.
     */
    void submitFetchItem(FetchItem fi) {
      String qid = _queuePartitioner.getQueueId(fi);
      if (qid == null)
        return;

      _fetchQueue.submit(qid,
                         new FetchRunnable(fi, this));
    }

    /**
     * Set the crawl delay for the queue that manages the given FetchItem.
     *
     * @param delay the crawl delay in milliseconds
     */
    void setCrawlDelay(FetchItem fi, long delay) {
      String qid = _queuePartitioner.getQueueId(fi);
      if (qid == null)
        return;

      _fetchQueue.getQueue(qid).setCrawlDelay(delay);
    }

    /**
     * Entry point for Mapper
     */
    public void run(RecordReader<Text, CrawlDatum> input,
                    OutputCollector<Text, NutchWritable> output,
                    Reporter reporter) throws IOException {

      Text key = new Text();
      CrawlDatum val = new CrawlDatum();

      _output = output;

      while (input.next(key, val)) {        
        FetchItem fi = new FetchItem(key, val);
        submitFetchItem(fi);

        // while there are more than _threadCount queues that have at least 2
        // items queued up, sleep for a little bit.
        while (_fetchQueue.countFullQueues(2) > _threadCount) {
          reporter.incrCounter(Counters.QUEUES_FULL_WAIT, 1);
          try {
            Thread.sleep(500);
          } catch (InterruptedException ie) {
          }          
        }
      }
    }

    /**
     * The Mapper has run through all of its input. We need to wait for all the
     * queues to empty out.
     */
    public void close() {
      // TODO log/status
      _fetchQueue.shutdown();
      _fetchQueue.awaitCompletion();
      _executor.shutdown();
    }

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      // TODO: add back error counter
      //      _errors.incrementAndGet();
    }

    /**
     * Output the fetched content for this CrawlDatum. If we have content fetched,
     * and we are running in parsing mode, the content parsed and the parse status is returned.
     */
    private ParseStatus output(Text key, CrawlDatum datum,
                               Content content, ProtocolStatus pstatus, int status) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

      ParseResult parseResult = null;      
      if (content != null) {
        Metadata metadata = content.getMetadata();
        // add segment to metadata
        metadata.set(Nutch.SEGMENT_NAME_KEY, _segmentName);
        // add score to content metadata so that ParseSegment can pick it up.
        try {
          _scFilters.passScoreBeforeParsing(key, datum, content);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            e.printStackTrace(LogUtil.getWarnStream(LOG));
            LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
          }
        }
        /* Note: Fetcher will only follow meta-redirects coming from the
         * original URL. */ 
        if (_isParsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
          try {
            parseResult = _parseUtil.parse(content);
          } catch (Exception e) {
            LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
          }

          if (parseResult == null) {
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, 
                                                                 new ParseStatus().getEmptyParse(getConf()));
            datum.setSignature(signature);
          }
        }
        
        /* Store status code in content So we can read this value during 
         * parsing (as a separate job) and decide to parse or not.
         */
        content.getMetadata().add(Nutch.FETCH_STATUS_KEY, Integer.toString(status));
      }

      try {
        _output.collect(key, new NutchWritable(datum));
        if (content != null && _isStoringContent)
          _output.collect(key, new NutchWritable(content));
        if (parseResult != null) {
          for (Entry<Text, Parse> entry : parseResult) {
            Text url = entry.getKey();
            Parse parse = entry.getValue();
            ParseStatus parseStatus = parse.getData().getStatus();
            
            if (!parseStatus.isSuccess()) {
              LOG.warn("Error parsing: " + key + ": " + parseStatus);
              parse = parseStatus.getEmptyParse(getConf());
            }

            // Calculate page signature. For non-parsing fetchers this will
            // be done in ParseSegment
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, parse);
            // Ensure segment name and score are in parseData metadata
            parse.getData().getContentMeta().set(Nutch.SEGMENT_NAME_KEY, 
                                                 _segmentName);
            parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY, 
                                                 StringUtil.toHexString(signature));
            // Pass fetch time to content meta
            parse.getData().getContentMeta().set(Nutch.FETCH_TIME_KEY,
                                                 Long.toString(datum.getFetchTime()));
            if (url.equals(key))
              datum.setSignature(signature);
            try {
              _scFilters.passScoreAfterParsing(url, content, parse);
            } catch (Exception e) {
              if (LOG.isWarnEnabled()) {
                e.printStackTrace(LogUtil.getWarnStream(LOG));
                LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
              }
            }
            _output.collect(url, new NutchWritable(
                              new ParseImpl(new ParseText(parse.getText()), 
                                            parse.getData(), parse.isCanonical())));
          }
        }
      } catch (IOException e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
        }
      }

      // return parse status if it exits
      if (parseResult != null && !parseResult.isEmpty()) {
        Parse p = parseResult.get(content.getUrl());
        if (p != null) {
          return p.getData().getStatus();
        }
      }
      return null;
    }

    private void updateStatus(int bytesInPage) throws IOException {
      // TODO: add back in status
      //pages.incrementAndGet();
      //bytes.addAndGet(bytesInPage);
    } 


    public int getMaxCrawlDelay() {
      return _maxCrawlDelay;
    }

    public int getMaxRedirects() {
      return _maxRedirects;
    }
  }

  /**
   * Interface for how to divide URLs into subqueues
   */
  private abstract static class QueuePartitioner {
    public String getQueueId(FetchItem item) {
      URL u = null;
      try {
        u = new URL(item.url.toString());
      } catch (Exception e) {
        LOG.warn("Cannot parse url: " + item.url, e);
        return null;
      }

      return getQueueIdForUrl(u);
    }

    abstract String getQueueIdForUrl(URL url);
  }


  /**
   * Partitions fetch items into queues by the IP address of their host
   */
  private static class ByHostnameQueuePartitioner extends QueuePartitioner {
    public String getQueueIdForUrl(URL u) {
      String proto = u.getProtocol().toLowerCase();
      String host = u.getHost();
      if (host == null) {
        LOG.warn("Unknown host for url: " + u + ", skipping.");
        return null;
      }
      host = host.toLowerCase();
      return proto + "://" + host;
    }
  }

  /**
   * Partitions fetch items into queues by the IP address of their host
   */
  private static class ByIpQueuePartitioner extends QueuePartitioner {
    public String getQueueIdForUrl(URL u) {
      String proto = u.getProtocol().toLowerCase();
      String host;
      try {
        InetAddress addr = InetAddress.getByName(u.getHost());
        host = addr.getHostAddress();
      } catch (UnknownHostException e) {
        // unable to resolve it, so don't fall back to host name
        LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
        return null;
      }
      return proto + "://" + host;
    }
  }

  /**
   * Fetch the given segment using the given number of threads.
   */
  public void fetch(Path segment, int threads)
    throws IOException {

    Configuration conf = getConf();
    FetcherConf.setThreads(conf, threads);
    fetch(segment);
  }

  /**
   * Fetch the given segment using the configured number of threads from our Configuration.
   */
  public void fetch(Path segment)
    throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: starting");
      LOG.info("Fetcher: segment: " + segment);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(InputFormat.class);

    job.setMapRunnerClass(FetchMapper.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: done"); }
  }


  public Fetcher() { super(null); }

  public Fetcher(Configuration conf) { super(conf); }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n] [-noParsing]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    Path segment = new Path(args[0]);

    Configuration conf = NutchConfiguration.create();

    for (int i = 1; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        FetcherConf.setThreads(conf, Integer.parseInt(args[++i]));
      } else if (args[i].equals("-noParsing"))
        FetcherConf.setParsing(conf, false);
    }

    Fetcher fetcher = new Fetcher(conf);          // make a Fetcher
    
    fetcher.fetch(segment);              // run the Fetcher

  }


}
