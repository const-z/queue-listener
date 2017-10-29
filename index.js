"use strict";

const EventEmitter = require("events").EventEmitter;
const {MongoClient, ObjectID } = require("mongodb");

const TASK_PROCESS_STATE = "task_process";
const TASK_DONE_STATE = "task_done";
const TASK_ERROR_STATE = "task_error";

const delay = (interval) => {
	return new Promise(resolve => {
		setTimeout(() => { resolve(); }, interval);
	});
};

const taskData = Symbol();

class Task extends EventEmitter {

	constructor(props) {
		super();
		this[taskData] = props;
	}

	get id() {
		return this[taskData]._id.toString();
	}

	set data(value) {
		this[taskData] = value;
	}

	get data() {
		return this[taskData];
	}

	done() {
		this.emit(TASK_DONE_STATE, { id: this.id });
	}

	error(error) {
		this.emit(TASK_ERROR_STATE, { id: this.id, error });
	}

	toJSON() {
		let { id, data } = this;
		return { id, data };
	}

}

const func_dbDisconnect = Symbol();
const func_dbConnect = Symbol();
const func_setState = Symbol();
const func_taskOnDone = Symbol();
const func_taskOnError = Symbol();
const func_reserveMessages = Symbol();
const func_createTasks = Symbol();
const func_listener = Symbol();
const queueData = Symbol();
const listenerId = Symbol();

class QueueListener extends EventEmitter {

	constructor(props) {
		super();
		this[queueData] = {};
		this[queueData].inProgress = false;
		this[queueData].active = false;
		this[queueData].collectionName = props.db.collection;
		this[queueData].dbUrl = props.db.url;
		this[queueData].delay = props.delay || 1000;
		this[queueData].limit = props.limit || 5;
		this[queueData].tasks = [];

		this[func_taskOnDone] = this[func_taskOnDone].bind(this);
		this[func_taskOnError] = this[func_taskOnError].bind(this);
		this[func_setState] = this[func_setState].bind(this);

		this[listenerId] = `${process.pid}-${new ObjectID()}`;
	}

	get id() {
		return this[listenerId];
	}

	async [func_dbConnect]() {
		this[queueData]._db = await MongoClient.connect(this[queueData].dbUrl, { autoReconnect: true, keepAlive: 120 });
		this[queueData]._collection = await this[queueData]._db.createCollection(this[queueData].collectionName);
		return;
	}

	async [func_dbDisconnect]() {
		if (this[queueData]._db.serverConfig.isConnected()) {
			try { await this[queueData]._db.close(); } catch (err) { null; }
		}
		return;
	}

	async [func_setState](taskId, state) {
		try {
			await this[queueData]._collection.update(
				{ _id: new ObjectID(taskId) },
				{ $set: { queueState: state, endProc: new Date() }, $unset: { listenerId: true } }
			);
		} catch (err) {
			null;
		}
		return;
	}

	async [func_taskOnDone](task) {
		await this[func_setState](task.id, TASK_DONE_STATE);
		this[queueData].tasks.splice(this[queueData].tasks.indexOf(task.id), 1);
		return;
	}

	async [func_taskOnError](task) {
		await this[func_setState](task.id, TASK_ERROR_STATE);
		this[queueData].tasks.splice(this[queueData].tasks.indexOf(task.id), 1);
		return;
	}

	async [func_reserveMessages]() {
		let messages = await this[queueData]._collection.find({
			queueState: { $nin: [TASK_DONE_STATE, TASK_ERROR_STATE] },
			$or: [{ listenerId: null }, { listenerId: this[listenerId] }]
		}).limit(this[queueData].limit).toArray();
		messages = messages.filter(m => !m.queueState || (m.queueState === TASK_PROCESS_STATE && !m.listenerId));
		const messagesIds = messages.map(m => m._id);
		await this[queueData]._collection.update(
			{
				_id: { $in: messagesIds },
				listenerId: null,
				$or: [{ queueState: null }, { $and: [{ queueState: TASK_PROCESS_STATE }, { listenerId: null }] }]
			},
			{ $set: { listenerId: this[listenerId], queueState: TASK_PROCESS_STATE, startProc: new Date() } },
			{ multi: true }
		);
		messages = await this[queueData]._collection.find({
			_id: { $in: messagesIds },
			listenerId: this[listenerId]
		}).limit(this[queueData].limit).toArray();
		return messages;
	}

	[func_createTasks](messages) {
		return messages.map(m => {
			const task = new Task(m);
			this[queueData].tasks.push(task.id);
			task.once(TASK_DONE_STATE, this[func_taskOnDone]);
			task.once(TASK_ERROR_STATE, this[func_taskOnError]);
			return task;
		});
	}

	async [func_listener]() {
		if (!this[queueData].active) {
			await this[func_dbDisconnect]();
			this.emit("stop");
			return;
		}
		try {
			this[queueData].inProgress = true;
			let messages = await this[func_reserveMessages]();
			const tasks = this[func_createTasks](messages);
			if (tasks.length) {
				this.emit("tasks", tasks);
			}
			await delay(this[queueData].delay);
		} catch (err) {
			await this[func_dbDisconnect]();
			if (!this[queueData].active) {
				try { await this[func_dbConnect](); } catch (err) { null; }
			}
			console.error(err);
		} finally {
			this[queueData].inProgress = false;
		}
		return this[func_listener]();
	}

	async start() {
		await this[func_dbConnect]();
		await this[queueData]._collection.update({}, { $unset: { listenerId: true } }, { multi: true });
		this[queueData].active = true;
		this[func_listener]();
		return;
	}

	async stop() {
		return await new Promise(resolve => {
			this[queueData].active = false;
			this.once("stop", () => {
				this[queueData].inProgress = false;
				resolve();
			});
		});
	}

}

module.exports = QueueListener;
