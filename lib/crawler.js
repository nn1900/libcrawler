/**
 * @file contains a general crawler implementation.
 * @author: eric <eric.blueplus@gmail.com>
 */

'use strict';

const request = require('request');
const EventEmitter = require('events').EventEmitter;
const inherits = require('util').inherits;
const lodash = require('lodash');
const it = require('../util/it');
const guid = require('../util/guid');
const Pool = require('./Pool');

/**
 * A general crawler class for a root url.
 * @constructor
 * @param {String} rootUrl the top level (root) url for this crawler.
 */
function Crawler(rootUrl, options) {
  if (typeof rootUrl === 'object') {
    options = rootUrl;
    rootUrl = options.rootUrl;
  }

  this.options = Object.assign({
    requestTimeout: 10000
  }, options);

  this.rootUrl = rootUrl;

  this.parallelTasks = {};
}

function taskFromItem(item, index, context) {
  const url = typeof item === 'string' ? item : item.url;
  return { retry: 0, item, index, url, context };
}

inherits(Crawler, EventEmitter);

/**
 * Start crawling.
 */
Crawler.prototype.start = function() {
  this.fetch(this.rootUrl, { isRootUrl: true });
}

/**
 * Fetch the content of the given url.
 */
Crawler.prototype.fetch = function(url, callback, context) {
  if (typeof callback !== 'function') {
    context = callback;
    callback = null;
  }
  const task = { url, context };
  this.tryLoadFromCache(
    context.cache,
    task,
    cb => {
      this.emit('urlLoading', task);
      this.get(url, cb);
    },
    err => {
      callback && callback(err, task);
      this.emit('urlDone', err, task);
    }
  );
}

Crawler.prototype._preprocessArgs = function(list, options, callback, defaults) {
  const type = Object.prototype.toString.call(list);

  if (typeof options === 'function') {
    callback = options;
    options = null;
  } else if (options && type !== '[object Array]') {
    throw new Error('the first argument should be a list. ');
  }

  if (type === '[object Object]') {
    options = list;
    list = options.list;
    delete options.list;

    if (!list) {
      throw new Error('missing list property in options');
    }
  }

  options = Object.assign({}, defaults, options);

  return {
    list: list,
    options: options,
    callback: callback
  };
}

/**
 * Try load task from the given cache.
 * @param {Object|Function} cache get function or an object w/ a `get`
 *   function to load content from cache.
 * @param {Object} task the task to load.
 * @param {Function} load function to load real data
 * @param {Function} callback function to called if data is loaded from cache.
 */
Crawler.prototype.tryLoadFromCache = function(
  cache, task, load, callback
) {
  function cb(err, response) {
    task.response = response;
    callback(err);
  }

  if (!cache) {
    load(cb);
    return;
  }

  const get = typeof cache === 'function' ? cache : cache.get;
  if (!get) {
    load(cb);
    return;
  }

  get(task, content => {
    if (!content) {
      load(cb);
    } else {
      task.response = {
        body: content,
        fromCached: true
      };
      callback(null);
    }
  });
}

/**
 * Crawl the given list of urls sequentially.
 * @param {Array|Object} list the url list or options object that
 *   contains the options and a `list` property to indicate the list of urls.
 * @param {Object|Function} [options] the options or callback function.
 * @param {Function} [callback] the callback function.
 */
Crawler.prototype.sequence = function(list, options, callback) {
  const args = this._preprocessArgs(list, options, callback, {
    breakOnError: true
  });

  list = args.list;
  options = args.options;
  callback = args.callback;

  // log the results for each url.
  const results = [];

  // begin load the contents of the urls in the list sequentially.
  it(list, (item, i, next) => {
    // process the url.
    var task = taskFromItem(item, i, options.context);
    this.tryLoadFromCache(
      options.cache,
      task,
      cb => {
        const url = task.url;
        this.emit('urlLoading', task);
        this.get(url, cb);
      },
      err => {
        this.emit('urlDone', err, task);
        results.push(task);
        if (err && options.breakOnError) {
          next(false);
        } else {
          next();
        }
      }
    );
  }, () => {
    callback && callback(results);
  });
}

// fork a new worker pool for a parallel task.
Crawler.prototype._forkPool = function(options, cache, maxRetry) {
  const pool = Pool.create(options);

  pool.on('task_proc', task => {
    // check the response from the cache where possible
    this.tryLoadFromCache(
      cache,
      task,
      cb => { // do real load
        const url = task.url;
        this.emit('urlLoading', task);
        this.get(url, cb, req => {
          task.req = req;
          task.abort = () => req.abort();
        });
      },
      err => {
        task.req = null;
        pool.done(task, err);
      }
    );
  });

  pool.on('task_done', (task, err) => {
    if (err && task.retry <= maxRetry) {
      this.emit('urlRetry', task);
      task.retry++;
      pool.enqueue(task);
      return;
    }
    this.emit('urlDone', err, task);
  });

  return pool;
}
/**
 * Crawl the given list of urls parallelly.
 * @param {Array|Object} list the url list or options object that
 *   contains the options and a `list` property to indicate the list of urls.
 * @param {Object|Function} [options] the options or callback function.
 * @param {Function} [callback] the callback function.
 */
Crawler.prototype.parallel = function(list, options, callback) {
  const args = this._preprocessArgs(list, options, callback, {
    maxQueueSize: 0, // unlimited
    maxWorkerSize: 10,
    maxRetry: 3
  });

  list = args.list;
  options = args.options;
  callback = args.callback;

  if (!list.length) {
    callback && callback();
    return;
  }

  // use a worker pool to manage the parallel task execution.
  const pool = this._forkPool(
    lodash.pick(options, [
    'maxQueueSize',
    'maxWorkerSize',
    'idleTimeout'
    ]), options.cache, options.maxRetry
  );

  pool.start();

  list.forEach((item, i) => {
    const task = taskFromItem(item, i, options.context);
    pool.enqueue(task);
  });

  pool.on('idle', () => {
    pool.stop();
    callback && callback();
  });
}

/**
 * Begin a new parallel task group w/ the given options.
 * @param {Object} options parallel task options.
 * @returns {String} parallel task group id.
 */
Crawler.prototype.beginParallel = function(options) {
  const groupId = guid('n').substr(8, 16);

  const poolOptions = lodash.pick(options, [
    'maxQueueSize',
    'maxWorkerSize',
    'idleTimeout'
  ]);

  const pool = this._forkPool(
    poolOptions,
    options.cache,
    options.maxRetry
  );

  this.parallelTasks[groupId] = {
    options,
    pool
  };

  return groupId;
}

/**
 * Add a task to a parallel task group.
 * @param {String} groupId id of the parallel task group.
 * @param {Object|String} taskInfo the task to add to the group.
 * @param {Object} [context] the context of this task.
 * @returns
 */
Crawler.prototype.addParallelTask = function(
  groupId, taskInfo, context
) {
  const taskGroup = this.parallelTasks[groupId];
  if (!taskGroup) {
    throw new Error(
      `Parallel task task '${groupId}' does not exist. `
    );
  }

  const url = typeof taskInfo === 'string' ?
    taskInfo : taskInfo.url;

  if (!url) {
    throw new Error('url is required for a task. ');
  }

  const task = {
    url: task.url,
    item: taskInfo,
    index: taskInfo.index,
    context
  };

  const pool = taskGroup.pool;
  pool.enqueue(task);

  return task;
}

/**
 * End a parallel task group identified by the `groupId`.
 * @param {String} groupId the id of the task to end.
 */
Crawler.prototype.endParallel = function(groupId) {
  const task = this.parallelTasks[groupId];
  if (!task) {
    throw new Error(
      `Parallel task task '${groupId}' does not exist. `
    );
  }
  task.pool.stop();
  delete this.parallelTasks[groupId];
}

/**
 * Get the content of the given url.
 * @param {String} url the url to request content.
 * @param {Function} [callback] callback
 * @returns if `callback` is not specified, returns a `Promise` instead.
 */
Crawler.prototype.get = function(url, callback, onRequest) {
  if (!callback) {
    return new Promise((resolve, reject) => {
      const req = request.get({
        url: url,
        timeout: this.options.requestTimeout
      },
      function(err, response, body) {
        if (err) reject(err);
        else {
          response.body = body;
          resolve(response);
        }
        req.destroy();
      });
      onRequest && onRequest(req);
    });
  } else {
    const req = request.get({
      url: url,
      timeout: this.options.requestTimeout
    }, function(err, response, body) {
      if (response) {
        response.body = body;
      }
      callback(err, response);
      req.destroy();
    });
    onRequest && onRequest(req);
  }
}

module.exports = Crawler;
