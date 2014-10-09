/**
 *  thread_safety.h
 *  express
 *
 *  Created by Adam Roberts on 1/25/12.
 *  Copyright 2012 Adam Roberts. All rights reserved.
 */

#ifndef express_thread_safety_h
#define express_thread_safety_h

#include <boost/thread.hpp>
#include <queue>

class Fragment;
class ReadHit;

/**
 * The ThreadSafeFragQueue is a threadsafe queue of Fragment pointers.
 *  @author    Adam Roberts
 *  @date      2012
 *  @copyright Artistic License 2.0
 **/
class ThreadSafeFragQueue {
  /**
   * A private queue of Fragment objects.
   */
  std::queue<Fragment*> _queue;
  /**
   * A private size_t representing the number of Fragments allowed in the queue
   * before blocking on a push.
   */
  size_t _max_size;
  /**
   * A private mutex used to provide thread-safety and used in association with
   * _cond to provide blocking behavior.
   */
  boost::mutex _mut;
  /**
   * A private condition variable used with _mut for blocking when queue is
   * empty on pop or full on push.
   */
  boost::condition_variable _cond;

 public:
  /**
   * ThreadSafeFragQueue Constructor.
   * @param max_size a size_t representing the number of Fragments allowed in

   *        the queue before blocking on a push.
   */
  ThreadSafeFragQueue(size_t max_size);
  /**
   * A member function that pops the next Fragment pointer off the queue. If
   * the queue is empty, returns NULL if block is false, otherwise blocks until
   * one is available.
   * @param block a bool specifying whether or not the function should block if
   *        the queue is empty.
   * @return The next Fragment pointer on the queue or NULL if the queue is
   *         empty and block is false.
   */
  Fragment* pop(bool block=true);
  /**
   * A member function that pushes the given Fragment pointer onto the queue.
   * Blocks if the queue is full.
   * @param frag the Fragment pointer to push onto the queue.
   */
  void push(Fragment* frag);
  /**
   * A member function that returns true iff the queue is empty. If block is
   * true, the function blocks until the queue is empty and returns true.
   * @param block a bool specifying whether or not the function should block
   *        until the queue is empty.
   * @return True iff the queue is empty.
   */
  bool is_empty(bool block=false);
};

/**
 * The ThreadSafeInvalidQueue is a threadsafe queue of invalid ReadHit pointers.
 *  @author    Richard Smith-Unna
 *  @date      2014
 *  @copyright Artistic License 2.0
 **/
class ThreadSafeInvalidQueue {
  /**
   * A private queue of ReadHit objects containing invalid alignments.
   */
  std::queue<ReadHit*> _queue;
  /**
   * A private size_t representing the number of ReadHits allowed in the queue
   * before blocking on a push.
   */
  size_t _max_size;
  /**
   * A private mutex used to provide thread-safety and used in association with
   * _cond to provide blocking behavior.
   */
  boost::mutex _mut;
  /**
   * A private condition variable used with _mut for blocking when queue is
   * empty on pop or full on push.
   */
  boost::condition_variable _cond;

 public:
  /**
   * ThreadSafeInvalidQueue Constructor.
   * @param max_size a size_t representing the number of ReadHits allowed in

   *        the queue before blocking on a push.
   */
  ThreadSafeInvalidQueue(size_t max_size);
  /**
   * A member function that pops the next ReadHits pointer off the queue. If
   * the queue is empty, returns NULL if block is false, otherwise blocks until
   * one is available.
   * @param block a bool specifying whether or not the function should block if
   *        the queue is empty.
   * @return The next ReadHit pointer on the queue or NULL if the queue is
   *         empty and block is false.
   */
  ReadHit* pop(bool block=true);
  /**
   * A member function that pushes the given Fragment pointer onto the queue.
   * Blocks if the queue is full.
   * @param frag the Fragment pointer to push onto the queue.
   */
  void push(ReadHit* frag);
  /**
   * A member function that returns true iff the queue is empty. If block is
   * true, the function blocks until the queue is empty and returns true.
   * @param block a bool specifying whether or not the function should block
   *        until the queue is empty.
   * @return True iff the queue is empty.
   */
  bool is_empty(bool block=false);
};

/**
 * The ParseThreadSafety struct stores objects to allow for parsing to safely
 * occur on a separate thread from processing.
 *  @author    Adam Roberts & Richard Smith-Unna
 *  @date      2011 & 2014
 *  @copyright Artistic License 2.0
 **/
struct ParseThreadSafety {
  /**
   * A public ThreadSafeFragQueue of pointers to Fragments that have been parsed
   * but not pre-processed.
   */
  ThreadSafeFragQueue proc_in;
  /**
   * A public ThreadSafeFragQueue of pointers to Fragments that have been
   * pre-processed but not processed.
   */
  ThreadSafeFragQueue proc_on;
  /**
   * A public ThreadSafeFragQueue of pointers to Fragments that have been
   * processed but not post-processed.
   */
  ThreadSafeFragQueue proc_out;
  /**
   * A public ThreadSafeInvalidQueue of pointers to ReadHits that contain
   * invalid alignments that should not be processed at all.
   */
  ThreadSafeInvalidQueue proc_invalid;
  /**
   * PraseThreadSafety constructor intializes queues to the given size.
   * @param q_size the maximum size for the ThreadSafeFragQueues.
   */
  ParseThreadSafety(size_t q_size)
      : proc_in(q_size), proc_on(q_size),
        proc_out(q_size), proc_invalid(q_size) {
  }
};

#endif
