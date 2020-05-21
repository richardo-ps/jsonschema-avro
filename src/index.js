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

		const isRequired = jsonSchemaAvro._isRequired(required, item)

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
			return jsonSchemaAvro._convertComplexProperty(item, schema[item], isRequired, path)
		}
		else if (jsonSchemaAvro._isArray(schema[item])) {
			return jsonSchemaAvro._convertArrayProperty(item, schema[item], isRequired, path)
		}
		else if(jsonSchemaAvro._hasEnum(schema[item])){
			return jsonSchemaAvro._convertEnumProperty(item, schema[item], isRequired, path)
		}
		return jsonSchemaAvro._convertProperty(item, schema[item], jsonSchemaAvro._isRequired(required, item))
	})
}

jsonSchemaAvro._convertComplexProperty = (name, contents, required = false, parentPath=[]) => {

	const path = parentPath.slice().concat(name)

	prop = {
		name: name,
		doc: contents.description || '',
		type: ['null', {
			type: 'record',
			name: path.join('_') + '_record',
			fields: jsonSchemaAvro._convertProperties(contents.properties, contents.required, path)
		}]
	} 

	return prop

}

jsonSchemaAvro._convertArrayProperty = (name, contents, required = false, parentPath=[]) => {

	
	const path = parentPath.slice().concat(name)

	/* Fix for incorrectly specifying null default value for an object inside an array */

	if(Array.isArray(contents.items.type)){

		contents.items.type = contents.items.type.filter(type => type !== 'null')[0]
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
				: jsonSchemaAvro._convertProperty(name, contents.items, false, true)
		}
	}
}

jsonSchemaAvro._convertEnumProperty = (name, contents, required = false, parentPath=[]) => {

	const path = parentPath.slice().concat(name)
	const valid = contents.enum.every((symbol) => reSymbol.test(symbol))

	const prop = {
		name: name,
		doc: contents.description || ''
	}

	type = {
			type: 'enum',
			name: path.join('_') + '_enum',
			symbols: contents.enum
	}

	if (valid) {

		/* Check if null is included in enum values or if default value is set as null */

		if (contents.enum.includes("null") || 
			contents.enum.includes(null) || 
			contents.default == "null" || 
			contents.default == null) {

			prop.type = ['null', type]
			prop.default = null

		} else {

			if(contents.hasOwnProperty('default')){
				prop.default = contents.default
			}

			prop.type = type

		}

	} else {

		// Invalid enum
		prop.type = ["null", "string"]
		prop.default = null

	}

	return prop
}

jsonSchemaAvro._convertProperty = (name, value, required = false, isArrayProperty = false) => {
	let prop = {
		name: name,
		doc: value.description || ''
	}

	let types = []

	/* All fields are required in Avro by default.  If you want to make something optional, you have to make it nullable by unioning its type with null */
	
	// Check if field has been set as optional through type already

	if (Array.isArray(value.type)) {

		json_type = value.type.filter(type => type !== 'null')[0]
		avro_type = typeMapping[json_type]

		types = ['null', avro_type]
		prop.default = null // Remove any default value

	} else {

		// Check if field has been explicitly set as required

		if (required){

			json_type = value.type
			avro_type = typeMapping[json_type]


			types = [avro_type]

			// Use default value if provided

			if(value.hasOwnProperty('default')){

				if (value.default !== "null" || value.default !== null) {

					prop.default = value.default

				}
			}

		} else {

			// Set as optional

			json_type = value.type
			avro_type = typeMapping[json_type]

			types = ['null', avro_type]
			prop.default = null 

		}

	}

	if (isArrayProperty) {

		json_type = value.type
		avro_type = typeMapping[json_type]

		types = [avro_type]

		console.log('ARRAY of ', avro_type)

		delete prop.default

	}

	prop.type = types.length > 1 ? types : types.shift()

	//console.log('types', types)
	//console.log('size', types.length)	

	//console.log ('\n\n')
	//console.log('prop', prop)
	return prop

}
