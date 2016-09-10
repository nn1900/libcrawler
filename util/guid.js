function s4() {
  return Math.floor((1 + Math.random()) * 0x10000)
    .toString(16)
    .substring(1);
}

/**
 * Create a guid string.
 * @param {String} format if 'n' is specified the dash is ignored.
 * @returns guid in format of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx if 'n' is
 *   not specified otherwise returns the guid w/out the dashes.
 */
module.exports = function guid(format) {
  const dash = format && /^n$/i.test(format) ? '' : '-';
  return [
    s4(),
    s4(),
    dash,
    s4(),
    dash,
    s4(),
    dash,
    s4(),
    dash,
    s4(),
    s4(),
    s4()
  ].join('');
}
