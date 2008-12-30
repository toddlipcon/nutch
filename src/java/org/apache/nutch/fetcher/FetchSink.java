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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;

interface FetchSink {
  ParseStatus output(Text key, CrawlDatum datum,
                     Content content, ProtocolStatus pstatus, int status);

  void logError(Text url, String message);
  void updateStatus(int bytesInPage) throws IOException;
  void submitFetchItem(FetchItem item);
  void setCrawlDelay(FetchItem fi, long delay);
}