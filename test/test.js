"use strict";

const describe = require("mocha").describe;
const it = require("mocha").it;

const generate = require("./task-generator").generate;

describe("QueueListener", () => {

	const config = require("./config");
	const QueueListener = require("../");
	let queueListner1 = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });
	let queueListner2 = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });

	it("start 1", async () => {
		let tasksCount = await generate(34);
		console.log("task count", tasksCount);
		await Promise.all([new Promise((resolve) => {
			queueListner1.on("tasks", (tasks) => {
				tasks.map((task, i) => {
					console.log(JSON.stringify(task), "received");
					setTimeout(() => {
						tasksCount--;
						console.log(task.id, "queueListner1", "- done. left", tasksCount);
						task.done();
						if (tasksCount === 0) {
							resolve();
						}
					}, (i + 1) * 100);
				});
			});
			queueListner1.start();
		}), new Promise((resolve) => {
			queueListner2.on("tasks", (tasks) => {
				tasks.map((task, i) => {
					console.log(JSON.stringify(task), "received");
					setTimeout(() => {
						tasksCount--;
						console.log(task.id, "queueListner2", "- done. left", tasksCount);
						task.done();
						if (tasksCount === 0) {
							resolve();
						}
					}, (i + 1) * 100);
				});
			});
			queueListner2.start();
		})]);

		return await Promise.all([
			queueListner1.stop(),
			queueListner2.stop()
		]);
	});

	it.skip("start 2", async () => {
		let tasksCount = await generate(14);
		console.log("task count", tasksCount);
		queueListner1.removeAllListeners();
		await new Promise((resolve) => {
			queueListner1.on("tasks", (tasks) => {
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
			queueListner1.start();
		});

		return queueListner1.stop();
	});

});
