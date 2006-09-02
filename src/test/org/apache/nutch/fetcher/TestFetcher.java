package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;
import org.apache.nutch.crawl.CrawlDBTestUtil;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.protocol.Content;
import org.mortbay.jetty.Server;

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
    fs.delete(testdir);
    urlPath=new Path(testdir,"urls");
    crawldbPath=new Path(testdir,"crawldb");
    segmentsPath=new Path(testdir,"segments");
    server=CrawlDBTestUtil.getServer(conf.getInt("content.server.port",50000), "build/test/data/fetch-test-site");
    server.start();
  }

  protected void tearDown() throws InterruptedException, IOException{
    server.stop();
  }
  
  public void testFetch() throws IOException {
    
    //generate seedlist
    ArrayList<String> urls=new ArrayList<String>();
    
    addUrl(urls,"index.html");
    addUrl(urls,"pagea.html");
    addUrl(urls,"pageb.html");
    addUrl(urls,"dup_of_pagea.html");
    
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls);
    
    //inject
    Injector injector=new Injector(conf);
    injector.inject(crawldbPath, urlPath);

    //generate
    Generator g=new Generator(conf);
    Path generatedSegment=g.generate(crawldbPath, segmentsPath, 1, Long.MAX_VALUE, Long.MAX_VALUE);

    long time=System.currentTimeMillis();
    //fetch
    Fetcher fetcher=new Fetcher(conf);
    fetcher.fetch(generatedSegment, 1, true);

    time=System.currentTimeMillis()-time;
    
    //verify politeness, time taken should be more than (num_of_pages +1)*delay
    assertTrue(1000*time > (urls.size() + 1 * conf.getInt("fetcher.server.delay",5)));
    
    //verify results
    Path content=new Path(new Path(generatedSegment, Content.DIR_NAME),"part-00000/data");
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, content, conf);
    
    ArrayList<String> handledurls=new ArrayList<String>();
    
    READ:
      do {
      UTF8 key=new UTF8();
      Content value=new Content();
      if(!reader.next(key, value)) break READ;
      handledurls.add(key.toString());
    } while(true);

    reader.close();

    Collections.sort(urls);
    Collections.sort(handledurls);

    //verify that enough pages were handled
    assertEquals(urls.size(), handledurls.size());

    //verify that correct pages were handled
    assertTrue(handledurls.containsAll(urls));
    assertTrue(urls.containsAll(handledurls));

  }

  private void addUrl(ArrayList<String> urls, String page) {
    urls.add("http://127.0.0.1:" + server.getListeners()[0].getPort() + "/" + page);
  }

}
