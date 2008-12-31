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
  private static final String PARSE_KEY="fetcher.parse";
  private static final String THREADS_KEY="fetcher.threads.fetch";

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
    return (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
  }
  public static int getMaxCrawlDelay(Configuration conf) {
    return conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
  }

  public static int getThreadsPerHost(Configuration conf) {
    return conf.getInt("fetcher.threads.per.host", 1);
  }

  public static int getMaxRedirects(Configuration conf) {
    return conf.getInt("http.redirect.max", 3);
  }

  public static void setThreads(Configuration conf, int threads) {
    conf.setInt(THREADS_KEY, threads);
  }
}