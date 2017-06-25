"use strict";

const EventEmitter = require("events").EventEmitter;
const mongodb = require("mongodb");
const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");

const TASK_PROCESS = "task_process";
const TASK_DONE = "task_done";
const TASK_ERROR = "task_error";

const delay = (interval) => {
	return new Promise((resolve, reject) => {
		setTimeout(() => { resolve(); }, interval);
	});
};

class Task extends EventEmitter {

	constructor(props) {
		super();
		this.id = "" + props._id;
		this.data = props;
	}

	set data(value) {
		this.taskData = value;
	}

	get data() {
		return this.taskData;
	}

	done() {
		this.emit(TASK_DONE, { id: this.id });
	}

	error(error) {
		this.emit(TASK_ERROR, { id: this.id, error });
	}

}

class QueueListner extends EventEmitter {

	constructor(props) {
		super();
		this.work = false;
		this.collection = props.db.collection;
		this.dbUrl = props.db.url;
		this.delay = props.delay;
		this.limit = props.limit || 5;
		this.prev = null;
		this._taskOnDone = this._taskOnDone.bind(this);
		this._taskOnError = this._taskOnError.bind(this);
		this._setState = this._setState.bind(this);
	}

	async _setState(taskId, state) {
		let db = await MongoClient.connect(this.dbUrl);
		let collection = await db.createCollection(this.collection);
		await collection.update({ _id: new mongodb.ObjectID(taskId) }, { $set: { queueState: state } });
		await db.close();
		return;
	}

	_taskOnDone(task) {
		return this._setState(task.id, TASK_DONE);
	}

	_taskOnError(task) {
		return this._setState(task.id, TASK_ERROR);
	}

	start() {
		const listener = async () => {
			if (!this.work) { return; }
			let db = await MongoClient.connect(this.dbUrl);
			let collection = await db.createCollection(this.collection);
			let messages = await collection.find({ $or: [{ queueState: { $exists: false } }, { queueState: TASK_PROCESS }] }).limit(this.limit).toArray();
			messages = messages.filter(m => m.queueState !== TASK_PROCESS);
			let tasks = await Promise.all(messages.map(async m => {
				await collection.update({ _id: m._id }, { $set: { queueState: TASK_PROCESS } });
				m.queueState = TASK_PROCESS;
				let task = new Task(m);
				task.on(TASK_DONE, this._taskOnDone);
				task.on(TASK_ERROR, this._taskOnError);
				return task;
			}));

			this.emit("tasks", tasks);

			await db.close();
			await delay(this.delay);
			return listener();
		};
		this.work = true;
		listener();
	};

	stop() {
		this.work = false;
	}

}

const url = "mongodb://localhost:27017/popprod";
const collection = "messages";
const queueListner = new QueueListner({ db: { url, collection }, delay: 1000, limit: 1 });

queueListner.on("tasks", (tasks) => {
	tasks.map(item => {
		console.log(item.id);
		setTimeout(function() {
			item.done();
		}, 5000);
	});
});

queueListner.start();

