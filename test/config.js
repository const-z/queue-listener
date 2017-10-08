"use strict";

let config;

try {
	config = require("./config-custom");
} catch (err) {
	config = {
		database: {
			mongo: {
				url: "mongodb://localhost:27017/queue",
				collection: "messages"
			}
		}
	};
}

module.exports = config;
