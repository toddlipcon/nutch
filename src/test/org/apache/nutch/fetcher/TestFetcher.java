/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDBTestUtil;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.mortbay.jetty.Server;
import org.mortbay.http.HttpContext;

import junit.framework.TestCase;

/**
 * Basic fetcher test
 * 1. generate seedlist
 * 2. inject
 * 3. generate
 * 3. fetch
 * 4. Verify contents
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 *
 */
public class TestFetcher extends TestCase {

  final static Path testdir=new Path("build/test/fetch-test");
  Configuration conf;
  FileSystem fs;
  Path crawldbPath;
  Path segmentsPath;
  Path urlPath;
  Server server;

  protected void setUp() throws Exception{
    conf=CrawlDBTestUtil.createConfiguration();
    fs=FileSystem.get(conf);
    fs.delete(testdir, true);
    urlPath=new Path(testdir,"urls");
    crawldbPath=new Path(testdir,"crawldb");
    segmentsPath=new Path(testdir,"segments");

    server=CrawlDBTestUtil.getServer(conf.getInt("content.server.port",50000), "build/test/data/fetch-test-site");
  }

  protected void tearDown() throws InterruptedException, IOException{
    server.stop();
    fs.delete(testdir, true);
  }

  private List<String> getSeedList() {
    ArrayList<String> urls=new ArrayList<String>();
    
    addUrl(urls,"index.html");
    addUrl(urls,"pagea.html");
    addUrl(urls,"pageb.html");
    addUrl(urls,"dup_of_pagea.html");
    addUrl(urls,"nested_spider_trap.html");
    addUrl(urls,"exception.html");
    return urls;
  }

  private Path generateSegment(List<String> urls) throws IOException {
    //generate seedlist
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls);
    
    //inject
    Injector injector=new Injector(conf);
    injector.inject(crawldbPath, urlPath);

    //generate
    Generator g=new Generator(conf);
    Path generatedSegment = g.generate(crawldbPath, segmentsPath, 1,
        Long.MAX_VALUE, Long.MAX_VALUE, false, false);
    return generatedSegment;
  }

  private long timeFetch(Path segment) throws IOException {
    // Clean up previous fetch results so we can use the same inject and generate
    // twice.
    fs.delete(new Path(segment, "crawl_parse"));
    fs.delete(new Path(segment, "crawl_fetch"));
    fs.delete(new Path(segment, "parse_data"));
    fs.delete(new Path(segment, "parse_text"));

    long time=System.currentTimeMillis();
    Fetcher fetcher=new Fetcher(conf);
    fetcher.fetch(segment, 1);
    return System.currentTimeMillis()-time;
  }

  private List<CrawlDBTestUtil.RecordingHandler.LogEntry> crawlWithRobots(
    Path segment,String robotsTxt)
    throws Exception
  {
    // Set up the server so that it logs requests
    HttpContext context = server.getContext("/");
    CrawlDBTestUtil.RecordingHandler recorder = new CrawlDBTestUtil.RecordingHandler();
    context.addHandler(0, recorder);


    // Add a robots file which disallows us
    HashMap<String, String> robotMap = new HashMap<String, String>();
    robotMap.put("/robots.txt", "User-agent: *\nDisallow: /");
    context.addHandler(1, new CrawlDBTestUtil.StringServingHandler(robotMap));

    // Start up the server
    server.start();

    // Do the fetch
    long time = timeFetch(segment);

    server.stop();
    return recorder.getEntries();
  }

  public void testRobots() throws Exception {

    // We don't care about parsing output
    conf.setBoolean("fetcher.parse", false);

    List<String> urls = getSeedList();
    Path generatedSegment = generateSegment(urls);

    // See what got fetched
    List<CrawlDBTestUtil.RecordingHandler.LogEntry> entries = crawlWithRobots(
      generatedSegment, "User-agent: *\nDisallow: /");
    assertTrue(entries.size() > 0);

    // Make sure robots.txt got fetched
    assertEquals(entries.get(0).reqPath, "/robots.txt");

    // Make sure nothing else got fetched since we disallowed all user agents
    assertEquals(1, entries.size());
  }
  
  public void testFetch() throws Exception {
    server.start();
    
    List<String> urls = getSeedList();
    Path generatedSegment = generateSegment(urls);

    conf.setBoolean("fetcher.parse", true);

    long time = timeFetch(generatedSegment);
    
    //verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime=(int) ((urls.size()+1)*1000*conf.getFloat("fetcher.server.delay",5));
    assertTrue(time > minimumTime);
    
    //verify content
    Path content=new Path(new Path(generatedSegment, Content.DIR_NAME),"part-00000/data");
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, content, conf);
    
    ArrayList<String> handledurls=new ArrayList<String>();
    
    READ_CONTENT:
      do {
      Text key=new Text();
      Content value=new Content();
      if(!reader.next(key, value)) break READ_CONTENT;
      String contentString=new String(value.getContent());
      if(contentString.indexOf("Nutch fetcher test page")!=-1) { 
        handledurls.add(key.toString());
      }
    } while(true);

    reader.close();

    Collections.sort(urls);
    Collections.sort(handledurls);

    //verify that enough pages were handled
    assertEquals(urls.size(), handledurls.size());

    //verify that correct pages were handled
    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));
    
    handledurls.clear();

    //verify parse data
    Path parseData = new Path(new Path(generatedSegment, ParseData.DIR_NAME),"part-00000/data");
    reader = new SequenceFile.Reader(fs, parseData, conf);
    
    READ_PARSE_DATA:
      do {
      Text key = new Text();
      ParseData value = new ParseData();
      if(!reader.next(key, value)) break READ_PARSE_DATA;
      // make sure they all contain "nutch.segment.name" and "nutch.content.digest" 
      // keys in parse metadata
      Metadata contentMeta = value.getContentMeta();
      if (contentMeta.get(Nutch.SEGMENT_NAME_KEY) != null 
            && contentMeta.get(Nutch.SIGNATURE_KEY) != null) {
        handledurls.add(key.toString());
      }
    } while(true);
    
    Collections.sort(handledurls);

    assertEquals(urls.size(), handledurls.size());

    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));
  }

  private void addUrl(ArrayList<String> urls, String page) {
    urls.add("http://127.0.0.1:" + server.getListeners()[0].getPort() + "/" + page);
  }

}
