'use strict';

function newConfig() {
  return {
    dsn: 'postgresql://'
        + process.env['USER']
        + '@localhost/'
        + process.env['USER']
  }
}
exports.newConfig = newConfig;