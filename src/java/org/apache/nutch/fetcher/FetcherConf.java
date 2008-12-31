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


import org.apache.hadoop.conf.*;

/**
 * Utilities for getting fetcher-related configuration variables
 */
public abstract class FetcherConf {
  static final String PARSE_KEY="fetcher.parse";
  static final String THREADS_KEY="fetcher.threads.fetch";
  static final String SERVER_DELAY_KEY="fetcher.server.delay";
  static final String MIN_CRAWL_DELAY_KEY="fetcher.min.crawl.delay";
  static final String MAX_CRAWL_DELAY_KEY="fetcher.max.crawl.delay";
  static final String THREADS_PER_HOST_KEY="fetcher.threads.per.host";
  static final String MAX_REDIRECT_KEY="http.redirect.max";
  static final String ADAPTIVE_DELAY_KEY="fetcher.adaptive.crawl.delay.enabled";
  static final String ADAPTIVE_DELAY_RATIO_KEY="fetcher.adaptive.crawl.delay.ratio";
  static final String TERMINABLE_MIN_TIME_KEY="fetcher.terminable.min.request.time";

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean(PARSE_KEY, true);
  }

  public static void setParsing(Configuration conf, boolean val) {
    conf.setBoolean(PARSE_KEY, true);
  }

  public static int getThreads(Configuration conf) {
    return conf.getInt(THREADS_KEY, 10);
  }

  public static long getCrawlDelayMs(Configuration conf) {
    return (long) (conf.getFloat(SERVER_DELAY_KEY, 1.0f) * 1000);
  }

  public static int getMinCrawlDelay(Configuration conf) {
    return (int)(conf.getFloat(MIN_CRAWL_DELAY_KEY, 0.3f) * 1000);
  }

  public static int getMaxCrawlDelay(Configuration conf) {
    return conf.getInt(MAX_CRAWL_DELAY_KEY, 30) * 1000;
  }

  public static float getMinTerminableRequestTime(Configuration conf) {
    return conf.getFloat(TERMINABLE_MIN_TIME_KEY, 10.0f);
  }

  public static int getThreadsPerHost(Configuration conf) {
    return conf.getInt(THREADS_PER_HOST_KEY, 1);
  }

  public static int getMaxRedirects(Configuration conf) {
    return conf.getInt(MAX_REDIRECT_KEY, 3);
  }

  public static boolean isAdaptiveCrawlDelayEnabled(Configuration conf) {
    return conf.getBoolean(ADAPTIVE_DELAY_KEY, false);
  }

  public static float getAdaptiveCrawlDelayRatio(Configuration conf) {
    return conf.getFloat(ADAPTIVE_DELAY_RATIO_KEY, 5.0f);
  }

  public static void setThreads(Configuration conf, int threads) {
    conf.setInt(THREADS_KEY, threads);
  }
}