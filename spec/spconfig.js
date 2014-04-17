'use strict';

module.exports = {
  dsn: 'postgresql://' 
      + process.env['USER'] 
      + '@localhost/' 
      + process.env['USER']
}