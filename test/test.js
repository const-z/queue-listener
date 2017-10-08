"use strict";

const TestCase = require("mocha").describe;
const it = require("mocha").it;

TestCase("Queue listener", async () => {
	const tasksIds = [];
	const messageGenerator = async (urlDB, collectionName) => {
		const MongoClient = require("mongodb").MongoClient;
		let db = await MongoClient.connect(urlDB);
		let collection = await db.createCollection(collectionName);
		const e = Math.floor(Math.random() * (20 - 5) + 5);
		for (let i = 0; i < e; i++) {
			const t = await collection.insert({ name: "test" + Date.now() });
			tasksIds.push(t.insertedIds[0].toString());
		}
		await db.close();
	};

	it("should read all messages from queue", async () => {
		const config = require("./config");
		const { QueueListner } = require("../");
		let queueListner = new QueueListner({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 1000, limit: 5 });
		await messageGenerator(config.database.mongo.url, config.database.mongo.collection);
		await new Promise((resolve) => {
			queueListner.on("tasks", (tasks) => {
				tasks.map(task => {
					setTimeout(() => {
						tasksIds.splice(tasksIds.indexOf(task.id), 1);
						task.done();
						if (!tasksIds.length) {
							resolve();
						}
					}, 500);
				});
			});
			queueListner.start();
		});

		queueListner.stop();
		return;
	});

});
