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

package org.apache.nutch.segment;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

/** Dump the content of a segment. */
public class SegmentReader extends Configured implements Reducer {

  public static final Logger LOG = LogFormatter.getLogger(SegmentReader.class.getName());

  long recNo = 0L;
  
  private boolean co, fe, ge, pa, pd, pt;
  private FileSystem fs;

  /**
   * Wraps inputs in an {@link ObjectWritable}, to permit merging different
   * types in reduce.
   */
  public static class InputFormat extends SequenceFileInputFormat {
    public RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter)
            throws IOException {
      reporter.setStatus(split.toString());

      return new SequenceFileRecordReader(job, split) {
        public synchronized boolean next(Writable key, Writable value) throws IOException {
          ObjectWritable wrapper = (ObjectWritable) value;
          try {
            wrapper.set(getValueClass().newInstance());
          } catch (Exception e) {
            throw new IOException(e.toString());
          }
          return super.next(key, (Writable) wrapper.get());
        }
      };
    }
  }

  /** Implements a text output format */
  public static class TextOutputFormat extends org.apache.hadoop.mapred.OutputFormatBase {
    public RecordWriter getRecordWriter(final FileSystem fs, JobConf job, String name) throws IOException {

      final Path segmentDumpFile = new Path(job.getOutputPath(), name);

      // Get the old copy out of the way
      fs.delete(segmentDumpFile);

      final PrintStream printStream = new PrintStream(fs.create(segmentDumpFile));
      return new RecordWriter() {
        public synchronized void write(WritableComparable key, Writable value) throws IOException {
          ObjectWritable writable = (ObjectWritable) value;
          printStream.println((String) writable.get());
        }

        public synchronized void close(Reporter reporter) throws IOException {
          printStream.close();
        }
      };
    }
  }

  public SegmentReader() {
    super(null);
  }
  
  public SegmentReader(Configuration conf, boolean co, boolean fe, boolean ge, boolean pa,
          boolean pd, boolean pt) {
    super(conf);
    this.co = co;
    this.fe = fe;
    this.ge = ge;
    this.pa = pa;
    this.pd = pd;
    this.pt = pt;
    try {
      this.fs = FileSystem.get(getConf());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void configure(JobConf job) {
    setConf(job);
    this.co = getConf().getBoolean("segment.reader.co", true);
    this.fe = getConf().getBoolean("segment.reader.fe", true);
    this.ge = getConf().getBoolean("segment.reader.ge", true);
    this.pa = getConf().getBoolean("segment.reader.pa", true);
    this.pd = getConf().getBoolean("segment.reader.pd", true);
    this.pt = getConf().getBoolean("segment.reader.pt", true);
    try {
      this.fs = FileSystem.get(getConf());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private JobConf createJobConf() {
    JobConf job = new JobConf(getConf());
    job.setBoolean("segment.reader.co", this.co);
    job.setBoolean("segment.reader.fe", this.fe);
    job.setBoolean("segment.reader.ge", this.ge);
    job.setBoolean("segment.reader.pa", this.pa);
    job.setBoolean("segment.reader.pd", this.pd);
    job.setBoolean("segment.reader.pt", this.pt);
    return job;
  }
  
  public void close() {}

  public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter)
          throws IOException {
    StringBuffer dump = new StringBuffer();

    dump.append("\nRecno:: ").append(recNo++).append("\n");
    dump.append("URL:: " + key.toString() + "\n");
    while (values.hasNext()) {
      Object value = ((ObjectWritable) values.next()).get(); // unwrap
      if (value instanceof CrawlDatum) {
        dump.append("\nCrawlDatum::\n").append(((CrawlDatum) value).toString());
      } else if (value instanceof Content) {
        dump.append("\nContent::\n").append(((Content) value).toString());
      } else if (value instanceof ParseData) {
        dump.append("\nParseData::\n").append(((ParseData) value).toString());
      } else if (value instanceof ParseText) {
        dump.append("\nParseText::\n").append(((ParseText) value).toString());
      } else {
        LOG.warning("Unrecognized type: " + value.getClass());
      }
    }
    output.collect(key, new ObjectWritable(dump.toString()));
  }

  public void dump(Path segment, Path output) throws IOException {
    LOG.info("SegmentReader: dump segment: " + segment);

    JobConf job = createJobConf();
    job.setJobName("read " + segment);

    if (ge) job.addInputPath(new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    if (fe) job.addInputPath(new Path(segment, CrawlDatum.FETCH_DIR_NAME));
    if (pa) job.addInputPath(new Path(segment, CrawlDatum.PARSE_DIR_NAME));
    if (co) job.addInputPath(new Path(segment, Content.DIR_NAME));
    if (pd) job.addInputPath(new Path(segment, ParseData.DIR_NAME));
    if (pt) job.addInputPath(new Path(segment, ParseText.DIR_NAME));

    job.setInputFormat(InputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ObjectWritable.class);

    job.setReducerClass(SegmentReader.class);

    Path tempDir = new Path("/tmp/segread-" + new java.util.Random().nextInt());
    fs.delete(tempDir);
    
    job.setOutputPath(tempDir);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ObjectWritable.class);

    JobClient.runJob(job);

    // concatenate the output
    Path dumpFile = new Path(output, job.get("segment.dump.dir", "dump"));

    // remove the old file
    fs.delete(dumpFile);
    Path[] files = fs.listPaths(tempDir);

    PrintWriter writer = null;
    int currentRecordNumber = 0;
    if (files.length > 0) {
      writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(dumpFile))));
      try {
        for (int i = 0; i < files.length; i++) {
          Path partFile = (Path) files[i];
          try {
            currentRecordNumber = append(fs, job, partFile, writer, currentRecordNumber);
          } catch (IOException exception) {
            LOG.warning("Couldn't copy the content of " + partFile.toString() + " into " + dumpFile.toString());
            LOG.warning(exception.getMessage());
          }
        }
      } finally {
        writer.close();
      }
    }
    fs.delete(tempDir);
    LOG.info("SegmentReader: done");
  }

  /** Appends two files and updates the Recno counter */
  private int append(FileSystem fs, Configuration conf, Path src, PrintWriter writer, int currentRecordNumber)
          throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src)));
    try {
      String line = reader.readLine();
      while (line != null) {
        if (line.startsWith("Recno:: ")) {
          line = "Recno:: " + currentRecordNumber++;
        }
        writer.println(line);
        line = reader.readLine();
      }
      return currentRecordNumber;
    } finally {
      reader.close();
    }
  }

  private static final String[][] keys = new String[][] {
          {"co", "Content::\n"},
          {"ge", "Crawl Generate::\n"},
          {"fe", "Crawl Fetch::\n"},
          {"pa", "Crawl Parse::\n"},
          {"pd", "ParseData::\n"},
          {"pt", "ParseText::\n"}
  };

  public void get(final Path segment, final UTF8 key, Writer writer,
          final Map results) throws Exception {
    LOG.info("SegmentReader: get '" + key + "'");
    ArrayList threads = new ArrayList();
    if (co) threads.add(new Thread() {
      public void run() {
        try {
          List res = getMapRecords(new Path(segment, Content.DIR_NAME), key);
          results.put("co", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    if (fe) threads.add(new Thread() {
      public void run() {
        try {
          List res = getMapRecords(new Path(segment, CrawlDatum.FETCH_DIR_NAME), key);
          results.put("fe", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    if (ge) threads.add(new Thread() {
      public void run() {
        try {
          List res = getSeqRecords(new Path(segment, CrawlDatum.GENERATE_DIR_NAME), key);
          results.put("ge", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    if (pa) threads.add(new Thread() {
      public void run() {
        try {
          List res = getSeqRecords(new Path(segment, CrawlDatum.PARSE_DIR_NAME), key);
          results.put("pa", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    if (pd) threads.add(new Thread() {
      public void run() {
        try {
          List res = getMapRecords(new Path(segment, ParseData.DIR_NAME), key);
          results.put("pd", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    if (pt) threads.add(new Thread() {
      public void run() {
        try {
          List res = getMapRecords(new Path(segment, ParseText.DIR_NAME), key);
          results.put("pt", res);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    Iterator it = threads.iterator();
    while (it.hasNext()) ((Thread)it.next()).start();
    int cnt = 0;
    do {
      try {
        Thread.sleep(5000);
      } catch (Exception e) {};
      it = threads.iterator();
      while (it.hasNext()) {
        if (((Thread)it.next()).isAlive()) cnt++;
      }
      if (cnt > 0) System.err.println("(" + cnt + " to retrieve)");
    } while (cnt > 0);
    for (int i = 0; i < keys.length; i++) {
      List res = (List)results.get(keys[i][0]);
      if (res != null && res.size() > 0) {
        for (int k = 0; k < res.size(); k++) {
          writer.write(keys[i][1]);
          writer.write(res.get(k) + "\n");
        }
      }
      writer.flush();
    }
  }
  
  private List getMapRecords(Path dir, UTF8 key) throws Exception {
    MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, dir, getConf());
    ArrayList res = new ArrayList();
    Class keyClass = readers[0].getKeyClass();
    Class valueClass = readers[0].getValueClass();
    if (!keyClass.getName().equals("org.apache.hadoop.io.UTF8"))
      throw new IOException("Incompatible key (" + keyClass.getName() + ")");
    Writable value = (Writable)valueClass.newInstance();
    // we don't know the partitioning schema
    for (int i = 0; i < readers.length; i++) {
      if (readers[i].get(key, value) != null)
        res.add(value);
      readers[i].close();
    }
    return res;
  }

  private List getSeqRecords(Path dir, UTF8 key) throws Exception {
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(getConf(), dir);
    ArrayList res = new ArrayList();
    Class keyClass = readers[0].getKeyClass();
    Class valueClass = readers[0].getValueClass();
    if (!keyClass.getName().equals("org.apache.hadoop.io.UTF8"))
      throw new IOException("Incompatible key (" + keyClass.getName() + ")");
    Writable aKey = (Writable)keyClass.newInstance();
    Writable value = (Writable)valueClass.newInstance();
    for (int i = 0; i < readers.length; i++) {
      while (readers[i].next(aKey, value)) {
        if (aKey.equals(key))
          res.add(value);
      }
      readers[i].close();
    }
    return res;
  }

  public static class SegmentReaderStats {
    public long start = -1L;
    public long end = -1L;
    public long generated = -1L;
    public long fetched = -1L;
    public long fetchErrors = -1L;
    public long parsed = -1L;
    public long parseErrors = -1L;
  }
  
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  
  public void list(List dirs, Writer writer) throws Exception {
    writer.write("NAME\t\tGENERATED\tFETCHER START\t\tFETCHER END\t\tFETCHED\tPARSED\n");
    for (int i = 0; i < dirs.size(); i++) {
      Path dir = (Path)dirs.get(i);
      SegmentReaderStats stats = new SegmentReaderStats();
      getStats(dir, stats);
      writer.write(dir.getName() + "\t");
      if (stats.generated == -1) writer.write("?");
      else writer.write(stats.generated + "");
      writer.write("\t\t");
      if (stats.start == -1) writer.write("?\t");
      else writer.write(sdf.format(new Date(stats.start)));
      writer.write("\t");
      if (stats.end == -1) writer.write("?");
      else writer.write(sdf.format(new Date(stats.end)));
      writer.write("\t");
      if (stats.fetched == -1) writer.write("?");
      else writer.write(stats.fetched + "");
      writer.write("\t");
      if (stats.parsed == -1) writer.write("?");
      else writer.write(stats.parsed + "");
      writer.write("\n");
      writer.flush();
    }
  }
  
  public void getStats(Path segment, final SegmentReaderStats stats) throws Exception {
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(getConf(), new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    long cnt = 0L;
    UTF8 key = new UTF8();
    for (int i = 0; i < readers.length; i++) {
      while (readers[i].next(key)) cnt++;
      readers[i].close();
    }
    stats.generated = cnt;
    Path fetchDir = new Path(segment, CrawlDatum.FETCH_DIR_NAME);
    if (fs.exists(fetchDir) && fs.isDirectory(fetchDir)) {
      cnt = 0L;
      long start = Long.MAX_VALUE;
      long end = Long.MIN_VALUE;
      CrawlDatum value = new CrawlDatum();
      MapFile.Reader[] mreaders = MapFileOutputFormat.getReaders(fs, fetchDir, getConf());
      for (int i = 0; i < mreaders.length; i++) {
        while (mreaders[i].next(key, value)) {
          cnt++;
          if (value.getFetchTime() < start) start = value.getFetchTime();
          if (value.getFetchTime() > end) end = value.getFetchTime();
        }
        mreaders[i].close();
      }
      stats.start = start;
      stats.end = end;
      stats.fetched = cnt;
    }
    Path parseDir = new Path(segment, ParseData.DIR_NAME);
    if (fs.exists(fetchDir) && fs.isDirectory(fetchDir)) {
      cnt = 0L;
      long errors = 0L;
      ParseData value = new ParseData();
      MapFile.Reader[] mreaders = MapFileOutputFormat.getReaders(fs, parseDir, getConf());
      for (int i = 0; i < mreaders.length; i++) {
        while (mreaders[i].next(key, value)) {
          cnt++;
          if (!value.getStatus().isSuccess()) errors++;
        }
        mreaders[i].close();
      }
      stats.parsed = cnt;
      stats.parseErrors = errors;
    }
  }
  
  private static final int MODE_DUMP = 0;

  private static final int MODE_LIST = 1;

  private static final int MODE_GET = 2;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return;
    }
    int mode = -1;
    if (args[0].equals("-dump"))
      mode = MODE_DUMP;
    else if (args[0].equals("-list"))
      mode = MODE_LIST;
    else if (args[0].equals("-get")) mode = MODE_GET;

    boolean co = true;
    boolean fe = true;
    boolean ge = true;
    boolean pa = true;
    boolean pd = true;
    boolean pt = true;
    // collect general options
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-nocontent")) {
        co = false;
        args[i] = null;
      } else if (args[i].equals("-nofetch")) {
        fe = false;
        args[i] = null;
      } else if (args[i].equals("-nogenerate")) {
        ge = false;
        args[i] = null;
      } else if (args[i].equals("-noparse")) {
        pa = false;
        args[i] = null;
      } else if (args[i].equals("-noparsedata")) {
        pd = false;
        args[i] = null;
      } else if (args[i].equals("-noparsetext")) {
        pt = false;
        args[i] = null;
      }
    }
    Configuration conf = NutchConfiguration.create();
    final FileSystem fs = FileSystem.get(conf);
    SegmentReader segmentReader = new SegmentReader(conf, co, fe, ge, pa, pd, pt);
    // collect required args
    switch (mode) {
      case MODE_DUMP:
        String input = args[1];
        if (input == null) {
          System.err.println("Missing required argument: <segment_dir>");
          usage();
          return;
        }
        String output = args.length > 2 ? args[2] : null;
        if (output == null) {
          System.err.println("Missing required argument: <output>");
          usage();
          return;
        }
        segmentReader.dump(new Path(input), new Path(output));
        return;
      case MODE_LIST:
        ArrayList dirs = new ArrayList();
        for (int i = 1; i < args.length; i++) {
          if (args[i] == null) continue;
          if (args[i].equals("-dir")) {
            Path dir = new Path(args[++i]);
            Path[] files = fs.listPaths(dir, new PathFilter() {
              public boolean accept(Path pathname) {
                try {
                  if (fs.isDirectory(pathname)) return true;
                } catch (IOException e) {};
                return false;
              }
            });
            if (files != null && files.length > 0) {
              dirs.addAll(Arrays.asList(files));
            }
          } else dirs.add(new Path(args[i]));
        }
        segmentReader.list(dirs, new OutputStreamWriter(System.out, "UTF-8"));
        return;
      case MODE_GET:
        input = args[1];
        if (input == null) {
          System.err.println("Missing required argument: <segment_dir>");
          usage();
          return;
        }
        String key = args.length > 2 ? args[2] : null;
        if (key == null) {
          System.err.println("Missing required argument: <keyValue>");
          usage();
          return;
        }
        segmentReader.get(new Path(input), new UTF8(key), new OutputStreamWriter(System.out, "UTF-8"), new HashMap());
        return;
      default:
        System.err.println("Invalid operation: " + args[0]);
        usage();
        return;
    }
  }

  private static void usage() {
    System.err.println("Usage: SegmentReader (-dump ... | -list ... | -get ...) [general options]\n");
    System.err.println("* General options:");
    System.err.println("\t-nocontent\tignore content directory");
    System.err.println("\t-nofetch\tignore crawl_fetch directory");
    System.err.println("\t-nogenerate\tignore crawl_generate directory");
    System.err.println("\t-noparse\tignore crawl_parse directory");
    System.err.println("\t-noparsedata\tignore parse_data directory");
    System.err.println("\t-noparsetext\tignore parse_text directory");
    System.err.println();
    System.err.println("* SegmentReader -dump <segment_dir> <output> [general options]");
    System.err.println("  Dumps content of a <segment_dir> as a text file to <output>.\n");
    System.err.println("\t<segment_dir>\tname of the segment directory.");
    System.err.println("\t<output>\tname of the (non-existent) output directory.");
    System.err.println();
    System.err.println("* SegmentReader -list (<segment_dir1> ... | -dir <segments>) [general options]");
    System.err.println("  List a synopsis of segments in specified directories, or all segments in");
    System.err.println("  a directory <segments>, and print it on System.out\n");
    System.err.println("\t<segment_dir1> ...\tlist of segment directories to process");
    System.err.println("\t-dir <segments>\t\tdirectory that contains multiple segments");
    System.err.println();
    System.err.println("* SegmentReader -get <segment_dir> <keyValue> [general options]");
    System.err.println("  Get a specified record from a segment, and print it on System.out.\n");
    System.err.println("\t<segment_dir>\tname of the segment directory.");
    System.err.println("\t<keyValue>\tvalue of the key (url).");
    System.err.println("\t\tNote: put double-quotes around strings with spaces.");
  }
}
