const fs = require('fs')
const path = require('path')
const text = fs.readFileSync(
  path.join(__dirname, '..', '..', 'src', 'queries', 'scan-complete-eligible-users.sql'),
  'utf8',
)
module.exports = text
module.exports.default = text
