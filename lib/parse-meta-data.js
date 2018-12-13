const _ = require('lodash')

const parseMetaField = (elem, field, actionMeta) => {
  if (elem[field]) {
    actionMeta.index[field] = elem[field]
  }
  if (elem[`_${field}`]) {
    actionMeta.index[field] = elem[`_${field}`]
  }
}

const parseMetaFields = (metaFields, elem, actionMeta) => {
  metaFields.forEach(field => _.castArray(elem).forEach(el => parseMetaField(el, field, actionMeta)))
}

module.exports = {
  parseMetaFields
}
