function createLib (execlib) {
  'use strict';

  return execlib.loadDependencies('client', ['allex_leveldblib'], require('./creator').bind(null, execlib));
}

module.exports = createLib;
