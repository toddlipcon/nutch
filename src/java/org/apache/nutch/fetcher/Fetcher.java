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


  /**
   * The Mapper that runs over a single part file inside a segment, fetching
   * each of the CrawlDatums and outputting CrawlDatums for the fetched responses.
   */
  private static class FetchMapper extends Configured
    implements MapRunnable<Text, CrawlDatum, Text, NutchWritable>,
    FetchSink
  {
    private int _threadCount;
    private boolean _isParsing;
    private boolean _isStoringContent;
    private String _segmentName;

    private ExecutorService _executor;
    private FetchQueue _fetchQueue;
    private QueuePartitioner _queuePartitioner;

    private OutputCollector<Text, NutchWritable> _output;
    private Reporter _reporter;

    private ParseUtil _parseUtil;
    private ScoringFilters _scFilters;

    public static enum Counters {
      QUEUES_FULL_WAIT
    };

    public void configure(JobConf conf) {
      setConf(conf);

      _scFilters = new ScoringFilters(conf);
      _parseUtil = new ParseUtil(conf);

      _isParsing = FetcherConf.isParsing(conf);
      _isStoringContent = FetcherConf.isStoringContent(conf);

      _segmentName = conf.get(Nutch.SEGMENT_NAME_KEY);
      _threadCount = conf.getInt("fetcher.threads.fetch", 10);

      LOG.info("Fetcher: threads: " + _threadCount);

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
     * Start the ExecutorService on which the actual fetch jobs will run.
     */
    private ExecutorService startExecutor() {
      return Executors.newFixedThreadPool(_threadCount);
    }

    /**
     * Submit a FetchItem to its appropriate queue.
     */
    public void submitFetchItem(FetchItem fi) {
      String qid = _queuePartitioner.getQueueId(fi);
      if (qid == null)
        return;

      _fetchQueue.submit(qid,
                         new FetchRunnable(fi, getConf(), this));
    }

    /**
     * Set the crawl delay for the queue that manages the given FetchItem.
     *
     * @param delay the crawl delay in milliseconds
     */
    public void setCrawlDelay(FetchItem fi, long delay) {
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
      _reporter = reporter;

      while (input.next(key, val)) {        
        // The cloning below is very important - otherwise it will be modified
        // underneath the FetchItem.
        FetchItem fi = new FetchItem(new Text(key), (CrawlDatum)val.clone());
        submitFetchItem(fi);

        // while there are more than _threadCount queues that have at least 2
        // items queued up, sleep for a little bit.
        while (_fetchQueue.countFullQueues(2) > _threadCount) {
          reporter.incrCounter(Counters.QUEUES_FULL_WAIT, 1);
          reporter.progress();
          try {
            Thread.sleep(500);
          } catch (InterruptedException ie) {
          }          
        }
      }

      close();
    }

    /**
     * The Mapper has run through all of its input. We need to wait for all the
     * queues to empty out.
     */
    public void close() {
      // TODO log/status
      LOG.info("Fetcher shutting down fetch queue");
      _fetchQueue.shutdown();
      LOG.info("Fetcher awaiting fetch queue completion");
      _fetchQueue.awaitCompletion();
      LOG.info("Fetcher shutting down executor completion");
      _executor.shutdown();
    }

    public void logError(Text url, String message) {
      _reporter.progress();
      LOG.info("fetch of " + url + " failed with: " + message);
      // TODO: add back error counter
      //      _errors.incrementAndGet();
    }

    /**
     * Output the fetched content for this CrawlDatum. If we have content fetched,
     * and we are running in parsing mode, the content parsed and the parse status is returned.
     */
    public ParseStatus output(Text key, CrawlDatum datum,
                              Content content, ProtocolStatus pstatus, int status) {

      _reporter.progress();
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
          LOG.warn("Couldn't pass score, url " + key + " (" +
                   StringUtils.stringifyException(e) + ")");
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
          collectParseResult(key, datum, content, parseResult);
        }
      } catch (IOException e) {
        LOG.fatal("fetcher caught:"+ StringUtils.stringifyException(e));
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

    private void collectParseResult(Text key, CrawlDatum datum,
                                    Content content, ParseResult parseResult)
      throws IOException
    {
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
          LOG.warn("Couldn't pass score, url " + key +
                   " (" + StringUtils.stringifyException(e) + ")");
        }
        _output.collect(url, new NutchWritable(
                          new ParseImpl(new ParseText(parse.getText()), 
                                        parse.getData(), parse.isCanonical())));
      }
    }

    public void updateStatus(int bytesInPage) throws IOException {
      // TODO: add back in status
      //pages.incrementAndGet();
      //bytes.addAndGet(bytesInPage);
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

    LOG.info("Fetcher: starting");
    LOG.info("Fetcher: segment: " + segment);

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(UnsplitableSFInputFormat.class);

    job.setMapRunnerClass(FetchMapper.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    LOG.info("Fetcher: done");
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
