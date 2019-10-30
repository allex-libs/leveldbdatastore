function createJobs (execlib, jobondestroyablelib, leveldblib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    ret = {
    };

  require('./onldbdatastorecreator')(lib, jobondestroyablelib, ret);
  require('./fetchandreportmissingcreator')(lib, ret);
  require('./fetchercreator')(lib, ret);
  require('./selfkeysremovercreator')(lib, ret);
  require('./buildtempdbcreator')(lib, jobondestroyablelib, leveldblib, ret);

  return ret;
}

module.exports = createJobs;
