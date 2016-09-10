'use strict'; 

/**
 * A simple array iterator
 * @param {Array} array the array to iterate through
 * @param {Function} exec function that handles the current element
 * @param {Function} callback called when the iteration finished.
 */
function it(array, exec, callback) {
  let current = 0;

  function next() {
    if (current >= array.length) {
      callback(current);
    } else {
      exec(array[current], current, result => {
        current++;

        if (result === false) {
          // `false` to break the iteration.
          callback(current);
          return;
        }

        next();
      });
    }
  }

  next();
}

module.exports = it;
