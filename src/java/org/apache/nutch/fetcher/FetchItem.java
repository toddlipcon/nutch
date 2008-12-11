/**
 * Copyright 2005 The Apache Software Foundation
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

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


/**
 * This class described the item to be fetched.
 */
class FetchItem {    
  Text url;
  CrawlDatum datum;
  int redirectDepth;
  URL javaUrl;

  public FetchItem(Text url, CrawlDatum datum) throws MalformedURLException {
    this.url = url;
    this.datum = datum;
    this.redirectDepth = 0;
    this.javaUrl = new URL(url.toString());
  }

  public FetchItem(Text url, CrawlDatum datum, int redirectDepth) {
    this.url = url;
    this.datum = datum;
    this.redirectDepth = redirectDepth;
  }

  public CrawlDatum getDatum() {
    return datum;
  }

  public Text getUrl() {
    return url;
  }

  public String getUrlString() {
    return url.toString();
  }

  public int getRedirectDepth() {
    return redirectDepth;
  }

  public URL getJavaUrl() {
    return javaUrl;
  }

  public String toString() {
    return "FetchItem{\n" +
      "url = " + String.valueOf(url) + "\n" +
      "crawlDatum = " + String.valueOf(datum)
      + "}";
  }
}
