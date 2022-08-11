const _ = require('lodash')

const parseMetaField = (elem, field, actionMeta, bulkAction) => {
  if (elem[field]) {
    actionMeta[bulkAction][field] = elem[field]
  }
  if (elem[`_${field}`]) {
    actionMeta[bulkAction][field] = elem[`_${field}`]
  }
}

const parseMetaFields = (metaFields, elem, actionMeta, bulkAction) => {
  metaFields.forEach(field => _.chain(elem)
    .castArray()
    .compact()
    .each(el => parseMetaField(el, field, actionMeta, bulkAction))
    .value()
  )
}

module.exports = {
  parseMetaFields
}
