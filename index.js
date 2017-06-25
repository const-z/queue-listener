"use strict";

const EventEmitter = require("events").EventEmitter;
const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");

const delay = (interval) => {
	return new Promise((resolve, reject) => {
		setTimeout(() => { resolve(); }, interval);
	});
};

class MessageListener extends EventEmitter {

	constructor(props) {
		super();
		this.work = false;
		this.collection = props.db.collection;
		this.dbUrl = props.db.url;
		this.delay = props.delay;
	}

	start() {
		const listener = async () => {
			let db = await MongoClient.connect(this.dbUrl);
			let collection = await db.createCollection(this.collection);
			let messages = await collection.find({ $or: [{ processed: false }, { processed: { $exists: false } }] }, { _id: true }).limit(5).toArray();
			for (let m of messages) {
				await collection.update({ _id: m._id }, { $set: { processed: true } });
			}
			await db.close();
			await delay(this.delay);
			if (!this.work) { return; }
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
const messageListener = new MessageListener({ db: { url, collection }, delay: 1000 });
messageListener.start();
