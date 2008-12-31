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

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;


/**
 * A queue that jobs can be submitted to with a queue ID string.
 * Within each queue ID, only a certain number of jobs may be executing
 * concurrently.
 */
public class FetchQueue {
  
  private final Map<String, RunQueue> _subqueues = new ConcurrentHashMap<String, RunQueue>();

  /**
   * The pool on which the submitted jobs will run.
   */
  private final ExecutorService _pool;

  /**
   * true until shutdown() is called, at which point any further attempt
   * to submit jobs will result in IllegalStateException
   */
  private boolean _alive = true;


  /**
   * For each subqueue how many tasks may be running concurrently.
   */
  private int _maxTasksPerHost;

  /**
   * How much time is delayed in between the work performed by each task, in
   * milliseconds.
   *
   * If you have two tasks in a subqueue and crawl delay = 2, tasks
   * can still execute 1 second apart - both then both tasks will
   * wait 2 seconds before executing again.
   */
  private final long _initialCrawlDelay;

  private Configuration _conf;

  public FetchQueue(ExecutorService pool,
                    Configuration conf) {
    _pool = pool;
    _conf = conf;

    _maxTasksPerHost = FetcherConf.getThreadsPerHost(conf);
    _initialCrawlDelay = FetcherConf.getCrawlDelayMs(conf);
  }

  /**
   * Submit a job for execution in the named queue.
   */
  public synchronized void submit(String queueId, Runnable runnable) {
    checkAlive();

    getQueue(queueId).submit(runnable);
  }

  /**
   * Get a queue by ID
   */
  public synchronized RunQueue getQueue(String queueId) {
    if (!_subqueues.containsKey(queueId)) {
      _subqueues.put(queueId, new RunQueue(_pool));
    }
    return _subqueues.get(queueId);
  }

  /**
   * Mark pool as shut down.
   *
   * Any further submit() calls will result in IllegalStateException
   */
  public synchronized void shutdown() {
    checkAlive();

    _alive = false;
  }

  /**
   * Wait for all queues to be empty
   *
   * This doesn't have to be synchronized since we don't want submitters
   * to block against it. If this returns but you didn't call shutdown
   * first, there's no guarantee that all queues are empty.
   */
  public void awaitCompletion(Runnable idler) {
    // Wait for completion
    boolean done = false;
    while (!done) {
      try { Thread.sleep(500); idler.run(); } catch (InterruptedException ie) {}

      done = true;
      for (RunQueue q : _subqueues.values()) {
        if (!q.isEmpty()) {
          done = false;
          break;
        }
      }
    }
  }

  /**
   * Return the number of queues that have queued up more than queuelen
   * items.
   *
   * This should not be "trusted" and is advisory.
   */
  public int countFullQueues(int queuelen) {
    int count = 0;
    for (RunQueue queue : _subqueues.values()) {
      if (queue.isFull(queuelen))
        count++;
    }
    return count;
  }

  private void checkAlive() {
    if (!_alive) {
      throw new IllegalStateException("FetchQueue already shut down");
    }
  }

  /**
   * Return the total number of items enqueued
   */
  public int getTotalQueueLength() {
    int count = 0;
    for (RunQueue queue : _subqueues.values())
      count += queue._queueLen;
    return count;
  }

  /**
   * Return the number of tasks running
   */
  public int getRunningCount() {
    int count = 0;
    for (RunQueue queue : _subqueues.values())
      count += queue._tasksRunning;
    return count;
  }

  /**
   * Return true if every queue is terminable.
   */
  public boolean isEveryQueueTerminable() {
    for (RunQueue queue : _subqueues.values())
      if (!queue.isTerminable())
        return false;
    return true;
  }

  /**
   * Mark all of the queues as terminating so that they just go through their remaining
   * items with no delay, outputting them to be re-fetched in a later segment.
   */
  public void terminateQueues() {
    for (RunQueue queue : _subqueues.values())
      queue.terminate();
  }


  /**
   * Timer used by RunQueue below, but inner classes can't have
   * static members
   */
  private static Timer _tickTimer = new Timer(true);


  /**
   * A queue handling run items for a single host.
   */
  class RunQueue {
    private int _tasksRunning;

    /**
     * The time to wait in between requests in milliseconds
     */
    private long _crawlDelayMs;
    
    /**
     * Maintains a trailing average of how long requests are taking in this queue.
     * This is incrementally updated after each fetch using an exponential approach.
     */
    private double _trailingTimeAverage;

    /**
     * True if adaptive crawl delay is enabled for this queue. If the robots.txt causes
     * setCrawlDelay to be called, this is forced false so that adaptive delay is disabled
     * for this queue only.
     */
    private boolean _adaptiveDelayEnabled;


    /**
     * When computing the trailing average, the weight with which to factor in the
     * old measurement. A higher value here lends more stability to the value of
     * _trailingTimeAverage but means that it will approach new values slowly.
     *
     * We use separate constants for when the new value is increasing or decreasing
     * relative to the current average. To be polite we'd rather quickly slow down
     * and then slowly creep back up to a faster request rate when we notice congestion
     * on the server.
     */
    private static final double TRAIL_AVERAGE_WEIGHT_INCREASING = 0.4;
    private static final double TRAIL_AVERAGE_WEIGHT_DECREASING = 0.6;

    private final Executor _runPool;

    private Queue<Runnable> _itemQueue;

    /**
     * Save the number of items in _itemQueue since we need
     * it often for sensing backpressure, and it's O(n)
     * in LinkedList
     */
    private int _queueLen = 0;


    /**
     * Set to true when the fetcher is doing early termination.
     */
    private boolean _terminated = false;
    
    public RunQueue(Executor runPool) {
      _tasksRunning = 0;
      _itemQueue = new LinkedList<Runnable>();
      _runPool = runPool;
      _crawlDelayMs = _initialCrawlDelay;
      _trailingTimeAverage = _crawlDelayMs / 1000.0;
      _adaptiveDelayEnabled = FetcherConf.isAdaptiveCrawlDelayEnabled(_conf);
    }

    public synchronized boolean isEmpty() {
      return _tasksRunning == 0 && _itemQueue.isEmpty();
    }

    /**
     * For the purposes of sensing backpressure
     */
    public synchronized boolean isFull(int queuelen) {
      return _tasksRunning == _maxTasksPerHost && _itemQueue.size() >= queuelen;
    }


    /**
     * Return true if the request rate of this fetch queue is slower than the
     * minimum request rate configured, or if this queue is empty.
     */
    public boolean isTerminable() {
      if (isEmpty())
        return true;

      double reqTime = _trailingTimeAverage + (double)(calculateCrawlDelay() / 1000.0);
      return (reqTime > FetcherConf.getMinTerminableRequestTime(_conf));
    }

    public void terminate() {
      _terminated = true;
    }

    /**
     * Forces the crawl delay for this queue to the given number of milliseconds between
     * requests. This also disables adaptive crawl delay.
     */
    public void setCrawlDelay(long delayMs) {
      assert(delayMs >= 0);
      _crawlDelayMs = delayMs;
      _adaptiveDelayEnabled = false;
    }

    /**
     * Record how long it took to fetch an item.
     *
     * This updates the internal statistics for this queue to determine
     * whether:
     *  (a) the queue is going slow enough that it is "terminable"
     *  (b) the crawl delay (in the case that adaptive crawl delay is enabled)
     */
    public void recordFetchTime(long ms) {
      double sec = (double)ms / 1000.0;

      double weight = (sec > _trailingTimeAverage) ?
        TRAIL_AVERAGE_WEIGHT_INCREASING :
        TRAIL_AVERAGE_WEIGHT_DECREASING;

      _trailingTimeAverage =
        weight * _trailingTimeAverage + (1 - weight) * sec;
    }

    /**
     * The flow choices here are:
     *
     * FLOW A. There are no items currently in the queue and < max tasks running:
     *
     *  - submit adds the item to the queue
     *  - checkTasks increments tasksRunning and ticks
     *  - taskTick takes the item off the queue
     *  - it then schedules tick to run again after crawldelay
     *
     * FLOW B. Max tasks are running
     *
     *  - The item is added to the queue  
     *  - checkTasks does nothing
     *  - some time later, a task tick occurs
     *    - it pulls item off queue, submits it to the runer pool, and reschedules
     */
    public synchronized void submit(Runnable item) {
      _itemQueue.add(item);
      _queueLen++;
      startTaskIfNeeded();
    }

    /**
     * If we we can start another task, do so
     */
    private synchronized void startTaskIfNeeded() {
      if (_tasksRunning < _maxTasksPerHost) {
        _tasksRunning++;
        taskTick(); // this kicks off the loop
      }
    }

    private synchronized void taskTick() {
      //      System.err.println("task tick");
      Runnable item;
      if ((item = _itemQueue.poll()) != null) {
        _queueLen--;
        _runPool.execute(new RunAndReschedule(item));
      } else {
        //System.err.println("Task done");
        _tasksRunning--;
      }
    }

    /**
     * Return the current crawl delay in milliseconds.
     */
    private long calculateCrawlDelay() {
      long delay;

      if (_adaptiveDelayEnabled)
        delay = (long)(_trailingTimeAverage * FetcherConf.getAdaptiveCrawlDelayRatio(_conf) * 1000);
      else
        delay = _crawlDelayMs;

      if (delay < FetcherConf.getMinCrawlDelay(_conf))
        delay = FetcherConf.getMinCrawlDelay(_conf);

      return delay;
    }

    private synchronized void scheduleTask() {
      // Once we're terminated, we just want to blast through the remaining
      // items, since we're just marking them as retry, so politeness doesn't
      // apply.
      if (_terminated) {
        taskTick();
      } else {
        _tickTimer.schedule(new TimerTask() { public void run() { taskTick(); } },
                            calculateCrawlDelay());
      }
    }


    /**
     * Wraps actual run tasks so that the runer doesn't run schedule itself to run again
     * until after each run completes
     */
    private class RunAndReschedule implements Runnable {
      private Runnable _task;

      public RunAndReschedule(Runnable task) {
        _task = task;
      }

      public void run() {
        try {
          _task.run();
        } finally {
          scheduleTask();
        }
      }
    }
  }


  /*************************************************************/
  /* Test code
   * Expected output:
   *  a/a starting
   *   a/b starting
   *     b/a starting
   *      b/b starting
   *  a/a ending
   *   a/b ending
   *     b/a ending
   *      b/b ending
   *    a/c starting
   *      b/c starting
   *    a/c ending
   *      b/c ending
   *
  /*************************************************************/

  public static void main(String args[]) {
    ExecutorService pool = Executors.newFixedThreadPool(4);

    Configuration conf = new Configuration();
    conf.setInt(FetcherConf.THREADS_PER_HOST_KEY, 2);
    conf.set(FetcherConf.SERVER_DELAY_KEY, "1.0");
    FetchQueue q = new FetchQueue(pool, conf);
    
    q.submit("a", new TestPrinter("a/a"));
    q.submit("a", new TestPrinter(" a/b"));
    q.submit("a", new TestPrinter("  a/c"));

    q.submit("b", new TestPrinter("   b/a"));
    q.submit("b", new TestPrinter("    b/b"));
    q.submit("b", new TestPrinter("     b/c"));

    q.shutdown();
    q.awaitCompletion(new Runnable() {
        public void run() {
          System.err.println("Awaiting completion...");
        }
      });
    pool.shutdown();
  }

  private static class TestPrinter implements Runnable {
    private String _s;
    public TestPrinter(String s) {
      _s = s;
    }
    public void run() {
      System.err.println(_s + " starting");
      try { Thread.sleep(5000); } catch (InterruptedException ie) {}
      System.err.println(_s + " ending");
    }
  }
}