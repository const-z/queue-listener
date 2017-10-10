"use strict";

const describe = require("mocha").describe;
const it = require("mocha").it;

const generate = require("./task-generator").generate;

describe("QueueListener", () => {
	it("should read all messages from queue", async () => {
		let tasksCount = await generate(14);
		console.log("task count", tasksCount);
		const config = require("./config");
		const QueueListener = require("../");
		let queueListner = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });

		await new Promise((resolve) => {
			queueListner.on("tasks", (tasks) => {
				tasks.map((task, i) => {
					console.log(JSON.stringify(task), "received");
					setTimeout(() => {
						tasksCount--;
						console.log(task.id, "- done. left", tasksCount);
						task.done();
						if (tasksCount === 0) {
							resolve();
						}
					}, (i + 1) * 100);
				});
			});
			queueListner.start();
		});

		queueListner.stop();
		return;
	});

});
