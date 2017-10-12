"use strict";

const describe = require("mocha").describe;
const it = require("mocha").it;

const generate = require("./task-generator").generate;

describe("QueueListener", () => {

	const config = require("./config");
	const QueueListener = require("../");
	let queueListner = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });

	it("start 1", async () => {
		let tasksCount = await generate(14);
		console.log("task count", tasksCount);
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

		return queueListner.stop();		
	});

	it("start 2", async () => {
		let tasksCount = await generate(14);
		console.log("task count", tasksCount);
		queueListner.removeAllListeners();
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

		return queueListner.stop();		
	});

});
