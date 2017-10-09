"use strict";

const describe = require("mocha").describe;
const it = require("mocha").it;

const generate = require("./task-generator").generate;

describe("QueueListener", () => {
	it("should read all messages from queue", async () => {
		let tasksCount = await generate();
		const config = require("./config");
		const QueueListener = require("../").QueueListener;
		let queueListner = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });

		await new Promise((resolve) => {
			queueListner.on("tasks", (tasks) => {
				tasks.map(task => {
					console.log(task.id, "received");
					setTimeout(() => {
						task.done();
						if (!--tasksCount) {
							resolve();
						}
					}, 100);
				});
			});
			queueListner.start();
		});

		queueListner.stop();
		return;
	});

});
