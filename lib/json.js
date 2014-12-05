module.exports.parse = function(str) {
	var result;
	try { result = JSON.parse(str); }
	catch (e) { throw new Error('Failed to parse JSON (message: "' + e.message + '"). Source: ' + JSON.stringify(str)); }

	return result;
};