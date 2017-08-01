function createLib (execlib) {
  'use strict';

  return execlib.loadDependencies('client', ['allex_leveldblib', 'allex_leveldbjoinerlib'], require('./creator').bind(null, execlib));
}

module.exports = createLib;
