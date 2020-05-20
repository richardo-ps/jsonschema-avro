const url = require('url')
const jsonSchemaAvro = module.exports = {}

// Json schema on the left, avro on the right
const typeMapping = {
	'string': 'string',
	'null': 'null',
	'boolean': 'boolean',
	'integer': 'int',
	'number': 'float'
}

const reSymbol = /^[A-Za-z_][A-Za-z0-9_]*$/;

jsonSchemaAvro.convert = (jsonSchema) => {
	if(!jsonSchema){
		throw new Error('No schema given')
	}
	let record = {
		name: jsonSchemaAvro._idToName(jsonSchema.id) || 'main',
		type: 'record',
		doc: jsonSchema.description,
		fields: jsonSchema.properties ? jsonSchemaAvro._convertProperties(jsonSchema.properties, jsonSchema.required) : []
	}
	const nameSpace = jsonSchemaAvro._idToNameSpace(jsonSchema.id)
	if(nameSpace){
		record.namespace = nameSpace
	}
	return record
}

jsonSchemaAvro._idToNameSpace = (id) => {
	if(!id){
		return
	}
	const parts = url.parse(id)
	let nameSpace = []
	if(parts.host){
		const reverseHost = parts.host.split(/\./).reverse()
		nameSpace = nameSpace.concat(reverseHost)
	}
	if(parts.path){
		const splitPath = parts.path.replace(/^\//, '').replace('.', '_').split(/\//)
		nameSpace = nameSpace.concat(splitPath.slice(0, splitPath.length - 1))
	}
	return nameSpace.join('.')
}

jsonSchemaAvro._idToName = (id) => {
	if(!id){
		return
	}
	const parts = url.parse(id)
	if(!parts.path){
		return
	}
	return parts.path.replace(/^\//, '').replace('.', '_').split(/\//).pop()
}

jsonSchemaAvro._isComplex = (schema) => {
	return schema.type === 'object'
}

jsonSchemaAvro._isArray = (schema) => {
	return schema.type === 'array'
}

jsonSchemaAvro._hasEnum = (schema) => {
	return Boolean(schema.enum)
}

jsonSchemaAvro._isRequired = (list, item) => list.includes(item)

jsonSchemaAvro._convertProperties = (schema = {}, required = [], path=[]) => {
	return Object.keys(schema).map((item) => {

		/* Fix for incorrectly specifying null default values for arrays and objects */

		if(Array.isArray(schema[item]['type'])){

			if (schema[item]['type'].includes('object')) {

				schema[item]['type'] = 'object'

			}

			if (schema[item]['type'].includes('array')) {

				schema[item]['type'] = 'array'
				
			}
		}

		if(jsonSchemaAvro._isComplex(schema[item])){
			return jsonSchemaAvro._convertComplexProperty(item, schema[item], path)
		}
		else if (jsonSchemaAvro._isArray(schema[item])) {
			return jsonSchemaAvro._convertArrayProperty(item, schema[item], path)
		}
		else if(jsonSchemaAvro._hasEnum(schema[item])){
			return jsonSchemaAvro._convertEnumProperty(item, schema[item], path)
		}
		return jsonSchemaAvro._convertProperty(item, schema[item], jsonSchemaAvro._isRequired(required, item), path)
	})
}

jsonSchemaAvro._convertComplexProperty = (name, contents, parentPath=[]) => {

	const path = parentPath.slice().concat(name)
	return {
		name: name,
		doc: contents.description || '',
		type: ['null', {
			type: 'record',
			name: path.join('_') + '_record',
			fields: jsonSchemaAvro._convertProperties(contents.properties, contents.required, path)
		}]

	} 
}

jsonSchemaAvro._convertArrayProperty = (name, contents, parentPath=[]) => {

	const path = parentPath.slice().concat(name)

	/* Fix for incorrectly specifying null default value for an object inside an array */

	if (contents.items.type.includes('object')) {

		contents.items.type = "object"
	}

	return {
		name: name,
		doc: contents.description || '',
		type: {
			type: 'array',
			items: jsonSchemaAvro._isComplex(contents.items)
				? {
					type: 'record',
					name: path.join('_') + '_record',
					fields: jsonSchemaAvro._convertProperties(contents.items.properties, contents.items.required, path)
				}
				: jsonSchemaAvro._convertProperty(name, contents.items, parentPath)
		}
	}
}

jsonSchemaAvro._convertEnumProperty = (name, contents, parentPath=[]) => {
	const path = parentPath.slice().concat(name)
	const valid = contents.enum.every((symbol) => reSymbol.test(symbol))
	var type = ["null", "string"]

	if (valid) {

		/* Check if null is included in enum values or if default value is set as null */

		if (contents.enum.includes("null") || 
			contents.enum.includes(null) || 
			contents.default == "null" || 
			contents.default == null) {

			var type = ['null', {
					type: 'enum',
					name: path.join('_') + '_enum',
					symbols: contents.enum
				}]

		} else {

			var type = {
				type: 'enum',
				name: path.join('_') + '_enum',
				symbols: contents.enum
			}

		}

	}

	let prop = {
		name: name,
		doc: contents.description || '',
		type: type
	}
	if(contents.hasOwnProperty('default')){
		prop.default = contents.default
	}
	return prop
}

jsonSchemaAvro._convertProperty = (name, value, required = false) => {
	let prop = {
		name: name,
		doc: value.description || ''
	}
	let types = []
	if(value.hasOwnProperty('default')){
		//console.log('has a default')
		prop.default = value.default
	}
	else if(!required){
		//console.log('not required and has no default')
		prop.default = null
		types.push('null')
	}
	if(Array.isArray(value.type)){

		// Fix for newtonsoft schema/njsonschema .net library refusing to put the null before the type

        const stringArray = value.type

        const positionInArray = stringArray.indexOf('null');
        const isNullInArray = positionInArray !== -1;
        const isNullNotAtStart = positionInArray > 0;
        if (isNullInArray && isNullNotAtStart) {
            stringArray.splice(positionInArray, 1);
            stringArray.unshift('null');
        }

		types = types.concat(stringArray.filter(type => type !== 'null').map(type => typeMapping[type]))
	}
	else{
		types.push(typeMapping[value.type])
	}
	//console.log('types', types)
	//console.log('size', types.length)
	prop.type = types.length > 1 ? types : types.shift()
	//console.log('prop', prop)
	return prop
}
