function createLib (execlib) {
  'use strict';

  return execlib.loadDependencies('client', ['allex:leveldb:lib'], require('./creator').bind(null, execlib));
}

module.exports = createLib;
