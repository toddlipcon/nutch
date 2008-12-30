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

  public FetchQueue(ExecutorService pool,
                    int maxTasksPerHost,
                    long crawlDelay) {
    _pool = pool;
    _maxTasksPerHost = maxTasksPerHost;
    _initialCrawlDelay = crawlDelay;
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
   * Timer used by RunQueue below, but inner classes can't have
   * static members
   */
  private static Timer _tickTimer = new Timer(true);


  /**
   * A queue handling run items for a single host.
   */
  class RunQueue {
    private int _tasksRunning;
    private long _crawlDelay;

    private final Executor _runPool;

    private Queue<Runnable> _itemQueue;

    /**
     * Save the number of items in _itemQueue since we need
     * it often for sensing backpressure, and it's O(n)
     * in LinkedList
     */
    private int _queueLen = 0;
    
    public RunQueue(Executor runPool) {
      _tasksRunning = 0;
      _itemQueue = new LinkedList<Runnable>();
      _runPool = runPool;
      _crawlDelay = _initialCrawlDelay;
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

    public void setCrawlDelay(long delayMs) {
      assert(delayMs >= 0);
      _crawlDelay = delayMs;
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

    private synchronized void scheduleTask() {
      _tickTimer.schedule(new TimerTask() { public void run() { taskTick(); } },
                          _crawlDelay);
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
    
    FetchQueue q = new FetchQueue(pool, 2, 2000);
    
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