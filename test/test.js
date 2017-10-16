"use strict";

const describe = require("mocha").describe;
const it = require("mocha").it;

const generate = require("./task-generator").generate;

describe("QueueListener", () => {

	const config = require("./config");
	const QueueListener = require("../");
	let queueListener1 = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });
	let queueListener2 = new QueueListener({ db: { url: config.database.mongo.url, collection: config.database.mongo.collection }, delay: 10, limit: 3 });

	it("start 1", async () => {
		let tasksCount = await generate(34);
		console.log("task count", tasksCount);
		await Promise.race([
			new Promise((resolve) => {
				queueListener1.on("tasks", (tasks) => {
					tasks.map((task, i) => {
						console.log(JSON.stringify(task), "received");
						setTimeout(() => {
							tasksCount--;
							console.log(task.id, "queueListener1", "- done. left", tasksCount);
							task.done();
							if (tasksCount === 0) {
								resolve();
							}
						}, (i + 1) * 100);
					});
				});
				queueListener1.start();
			}), new Promise((resolve) => {
				queueListener2.on("tasks", (tasks) => {
					tasks.map((task, i) => {
						console.log(JSON.stringify(task), "received");
						setTimeout(() => {
							tasksCount--;
							console.log(task.id, "queueListener2", "- done. left", tasksCount);
							task.done();
							if (tasksCount === 0) {
								resolve();
							}
						}, (i + 1) * 100);
					});
				});
				queueListener2.start();
			})
		]);

		return Promise.all([
			queueListener1.stop(),
			queueListener2.stop()
		]);
	});

	it.skip("start 2", async () => {
		let tasksCount = await generate(14);
		console.log("task count", tasksCount);
		queueListener1.removeAllListeners();
		await new Promise((resolve) => {
			queueListener1.on("tasks", (tasks) => {
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
			queueListener1.start();
		});

		return queueListener1.stop();
	});

});
