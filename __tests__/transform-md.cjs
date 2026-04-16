/** Jest: import .md as default string (CJS). */
module.exports = {
  process(sourceText) {
    return {
      code: `module.exports = ${JSON.stringify(sourceText)};\nmodule.exports.default = module.exports;`,
    }
  },
}
