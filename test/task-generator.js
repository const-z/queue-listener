"use strict";

const config = require("./config");

const messageGenerator = async (count, urlDB, collectionName) => {
	const MongoClient = require("mongodb").MongoClient;
	let db = await MongoClient.connect(urlDB);
	let collection = await db.createCollection(collectionName);
	count = count || Math.floor(Math.random() * (15 - 5) + 5);
	for (let i = 0; i < count; i++) {
		await collection.insert({ name: "test" + Date.now() });
	}
	await db.close();
	return count;
};

const generate = async (count) => {
	return await messageGenerator(count, config.database.mongo.url, config.database.mongo.collection);
};

const simulate = async () => {
	setInterval(async () => {
		await messageGenerator(config.database.mongo.url, config.database.mongo.collection);
	}, 500);
};

module.exports = {
	generate, simulate
};