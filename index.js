"use strict";

const EventEmitter = require("events").EventEmitter;
const mongodb = require("mongodb");
const MongoClient = require("mongodb").MongoClient;

const TASK_PROCESS_STATE = "task_process";
const TASK_DONE_STATE = "task_done";
const TASK_ERROR_STATE = "task_error";

const delay = (interval) => {
	return new Promise(resolve => {
		setTimeout(() => { resolve(); }, interval);
	});
};

class Task extends EventEmitter {

	constructor(props) {
		super();
		this.data = props;
	}

	get id() {
		return this.data._id.toString();
	}

	set data(value) {
		this.taskData = value;
	}

	get data() {
		return this.taskData;
	}

	done() {
		this.emit(TASK_DONE_STATE, { id: this.id });
	}

	error(error) {
		this.emit(TASK_ERROR_STATE, { id: this.id, error });
	}

}

class QueueListener extends EventEmitter {

	constructor(props) {
		super();
		this.inProgress = false;
		this.active = false;
		this.collection = props.db.collection;
		this.dbUrl = props.db.url;
		this.delay = props.delay || 1000;
		this.limit = props.limit || 5;
		this._taskOnDone = this._taskOnDone.bind(this);
		this._taskOnError = this._taskOnError.bind(this);
		this._setState = this._setState.bind(this);
		this.tasks = [];
	}

	async _dbConnect() {
		this._db = await MongoClient.connect(this.dbUrl, { autoReconnect: true, keepAlive: 120 });
		this._collection = await this._db.createCollection(this.collection);
		return;
	}

	async _setState(taskId, state) {
		try {
			await this._collection.update({ _id: new mongodb.ObjectID(taskId) }, { $set: { queueState: state }, $unset: { listenerId: true } });
		} catch (err) {
			null;
		}
		return;
	}

	async _taskOnDone(task) {
		await this._setState(task.id, TASK_DONE_STATE);
		this.tasks.splice(this.tasks.indexOf(task.id), 1);
		return;
	}

	async _taskOnError(task) {
		await this._setState(task.id, TASK_ERROR_STATE);
		this.tasks.splice(this.tasks.indexOf(task.id), 1);
		return;
	}

	async start() {
		const listener = async () => {
			if (!this.active) {
				if (this._db.serverConfig.isConnected()) {
					try { await this._db.close(); } catch (err) { null; }
				}
				return;
			}
			try {
				this.inProgress = true;
				let messages = await this._collection.find({
					$and: [{
						queueState: { $nin: [TASK_DONE_STATE, TASK_ERROR_STATE] }
					}, {
						$or: [{ listenerId: null }, { listenerId: process.pid }]
					}]
				}).limit(this.limit).toArray();
				messages = messages.filter(m => !m.queueState || (m.queueState === TASK_PROCESS_STATE && !m.listenerId));
				await this._collection.update(
					{ _id: { $in: messages.map(m => m._id) }, listenerId: null, $or: [{ queueState: null }, { $and: [{ queueState: TASK_PROCESS_STATE }, { listenerId: null }] }] },
					{ $set: { listenerId: process.pid, queueState: TASK_PROCESS_STATE } },
					{ multi: true }
				);
				messages = await this._collection.find({ _id: { $in: messages.map(m => m._id) }, listenerId: process.pid }).limit(this.limit).toArray();
				let tasks = messages.map(m => {
					const task = new Task(m);
					this.tasks.push(task.id);
					task.on(TASK_DONE_STATE, this._taskOnDone);
					task.on(TASK_ERROR_STATE, this._taskOnError);
					return task;
				});
				if (tasks.length) {
					this.emit("tasks", tasks);
				}
				await delay(this.delay);
				return listener();
			} catch (err) {
				if (this._db.serverConfig.isConnected()) {
					try { this._db.close(); } catch (err) { null; }
				}
				try { await this._dbConnect(); } catch (err) { null; }
				console.error(err);
			} finally {
				this.inProgress = false;
			}
		};
		await this._dbConnect();
		await this._collection.update({}, { $unset: { listenerId: true } }, { multi: true });
		this.active = true;
		listener();
		return;
	}

	async stop() {
		if (!this.inProgress && this._db.serverConfig.isConnected()) {
			try { await this._db.close(); } catch (err) { null; }
		}
		this.active = false;
	}

}

module.exports = {
	QueueListener,
	Task
};