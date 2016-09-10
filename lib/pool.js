/**
 * @file contains a parallel worker pool implementation.
 * @author: eric <eric.blueplus@gmail.com>
 */

'use strict';

const EventEmitter = require('events').EventEmitter;
const guid = require('../util/guid');
const inherits = require('util').inherits;

const DEFAULT_MAX_QUEUE_SIZE = Infinity;
const DEFAULT_MAX_WORKER_SIZE = 3;
const TASK_ID = '_$$id$$_';

/**
 * Make error w/ the given code and messsage.
 */
function makeError(code, msg) {
  return Object.assign(new Error(msg), { code: code });
}

/**
 * @class A parallel worker pool class.
 */
function Pool(options) {
  options = Object.assign({}, options);
  if (0 === options.maxQueueSize) delete options.maxQueueSize;
  if (0 === options.maxWorkerSize) delete options.maxWorkerSize;
  this.options = Object.assign({}, Pool.defaults, options);
  this.queue = [];
  this.pendingTasks = [];
  this.status = 'unavailable';
  this.idleTimer = null;
}

inherits(Pool, EventEmitter);

/**
 * Create a new pool.
 */
Pool.create = function(options) {
  return new Pool(options);
};

Pool.defaults = {
  maxQueueSize: DEFAULT_MAX_QUEUE_SIZE,
  maxWorkerSize: DEFAULT_MAX_WORKER_SIZE,
  idleTimeout: 300 * 1000 // 5 mins
};

/**
 * Start the pool.
 */
Pool.prototype.start = function(callback) {
  const options = this.options;
  const bootstrap = options.bootstrap;
  if (bootstrap) {
    this.status = 'bootstraping';
    bootstrap(err => {
      if (err) {
        this.stop('error');
        callback && callback(err);
      } else {
        callback && callback();
        this.status = 'idle';
      }
    });
  } else {
    this.status = 'idle';
  }
}

/**
 * Stop the pool.
 */
Pool.prototype.stop = function(status) {
  const options = this.options;
  const dispose = options.dispose;

  if (this.idleTimer) {
    clearTimeout(this.idleTimer);
    this.idleTimer = null;
  }

  dispose && dispose();

  this.pendingTasks.forEach(task => {
    task.abort && task.abort();
    task.abort = void 0;
  });

  this.queue = [];
  this.pendingTasks = [];

  this.status = status || 'stopped';
}

/**
 * Enqueue a task to the worker pool.
 * @param {Object} task the task to enqueue.
 */
Pool.prototype.enqueue = function(task) {
  if (this.status === 'unavailable') {
    throw makeError(
      'instruct_retry',
      'Pool service unavailable. Please try again later. '
    );
  }
  const maxQueueSize = this.options.maxQueueSize;
  if (this.queue.length >= maxQueueSize) {
    throw makeError(
      'max_queue_size_exceeded',
      `max queue size (${maxQueueSize}) exceeded. `
    );
  }
  var taskId = guid('n').substr(8, 16);
  task[TASK_ID] = taskId;
  task.getId = function() { return taskId; };
  this.queue.push(task);
  this.next();
  return taskId;
}

/**
 * Get task status.
 */
Pool.prototype.getTaskStatus = function(id) {
  if (!id) {
    throw new Error('id must be specified. ');
  }

  if (typeof id === 'object') id = id[TASK_ID];

  if (this.pendingTasks.findIndex(x => x[TASK_ID] === id) >= 0) {
    return 'pending';
  } else if (this.queue.findIndex(x => x[TASK_ID] === id) >= 0) {
    return 'queued';
  }

  throw new Error('task not found');
}

Pool.prototype.next = function() {
  const options = this.options;

  if (!this.queue.length) {
    if (!this.pendingTasks.length) {
      this._enterIdleState();
    }
    return;
  }

  if (this.idleTimer) {
    clearTimeout(this.idleTimer);
    this.idleTimer = null;
  }

  if (this.status === 'idle') {
    this.status = 'running';
  }

  if (this.pendingTasks.length >= options.maxWorkerSize) {
    return;
  }

  const task = this.queue.shift();

  if (!task) {
    this.next();
    return;
  }

  this.pendingTasks.push(task);

  this.emit('task_proc', task);
}

Pool.prototype.done = function(task, error) {
  const index = this.pendingTasks.indexOf(task);

  if (index >= 0) {
    this.pendingTasks.splice(index, 1);
  }

  this.emit('task_done', task, error);

  if (error) {
    this.emit('task_error', task, error);
  }

  this.next();

  // this._immediate(() => this.next());
}

Pool.prototype._immediate = function(fn) {
  var timer = setTimeout(() => {
    clearTimeout(timer);
    timer = null;
    fn();
  }, 0);
}

/**
 * @private Enter the pool to idle state.
 */
Pool.prototype._enterIdleState = function() {
  if (this.status === 'idle') return;

  const options = this.options;

  this.idleTimer = setTimeout(() => {
    // when idle timer timeout, stop the queue
    // to release resources.
    this.emit('idle_timeout');
    this.stop();
  }, options.idleTimeout);

  this.status = 'idle';
  this.emit('idle');
  // this._immediate(() => this.emit('idle'));
}

module.exports = Pool;
