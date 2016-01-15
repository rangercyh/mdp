/*
broker采用router转发消息
内部维护worker的列表
主动发送心跳给worker
接收worker的心跳，便于及时发现worker断开，防止给已经挂掉的worker发消息
发送消息：[HEARTBEAT] [REPLY, workid, rid, data] [DISCONNECT, workid, rid] [REQUEST, rid, data] [DONE, '', rid];
接收消息：[REQUEST, server, rid, data] [REPLY, server, rid, data] [HEARTBEAT, server]
*/

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var zmq = require('zmq');

var MDP = {
	REQUEST: 1,
	REPLY: 2,
	HEARTBEAT: 3,
	DISCONNECT: 4,
	DONE: 5,
	NOEXISTS: 6
};
var REQUEST = {
	POINT: 1,
	BROADCAST: 2
};
var HEALTH_LIVENESS = 3;
var HEARTBEAT_TIME = 2500;

function send(msg) {
	if (!this.socket) {
		return;
	}
	this.socket.send(msg);
}

function allReceiveProc(rid) {
	var prop, clientid;
	for (prop in this.castmsg[rid]) {
		if (this.castmsg[rid].hasOwnProperty(prop)) {
			return;
		}
	}

	// 群发消息全部收到了通知客户端
	clientid = this.clientmap[rid];
	send.call(this, [clientid, MDP.DONE, '', rid]);
	delete this.clientmap[rid];
	delete this.castmsg[rid];
}

function routeReq(server, clientid, rid, data) {
	var workid, load = Number.MAX_VALUE, result, type, segment;
	do {
		if (this.workers.hasOwnProperty(server)) {
			// 找一个负载最小的worker
			for (workid in this.workers[server]) {
				if (this.workers[server].hasOwnProperty(workid)) {
					if ((this.workers[server][workid].liveness >= 0) &&
						(this.workers[server][workid].load < load)) {
						load = this.workers[server][workid].load;
						result = workid;
					}
				}
			}
			if (result) {
				++this.workers[server][result].load;
				this.reqmap[rid] = result;
				this.clientmap[rid] = clientid;
				send.call(this, [result, MDP.REQUEST, rid, data]);
			} else {
				// 一个worker也找不到
				break;
			}
			return;
		}

		// 查看是否群发
		segment = server.split('-');
		if ((segment.length === 2) && (segment[0] === 'all')) {
			if (this.workers.hasOwnProperty(segment[1])) {
				for (workid in this.workers[segment[1]]) {
					if (this.workers[segment[1]].hasOwnProperty(workid)) {
						if (this.workers[segment[1]][workid].liveness >= 0) {
							++this.workers[segment[1]][workid].load;
							// 添加群发记录
							if (!this.castmsg.hasOwnProperty(rid)) {
								this.castmsg[rid] = {};
								this.clientmap[rid] = clientid;
							}
							this.castmsg[rid][workid] = 1;
							send.call(this, [workid, MDP.REQUEST, rid, data]);
							result = workid;
						}
					}
				}
				if (!result) {
					console.log("don't find ", workid);
					break;
				}
			} else {
				console.log("don't find ", server, segment);
				break;
			}
			return;
		}

		// 如果没有指定类型，那么再看看是否是指定了serverid
		for (type in this.workers) {
			if (this.workers.hasOwnProperty(type)) {
				if (this.workers[type].hasOwnProperty(server)) {
					if (this.workers[type][server].liveness >= 0) {
						++this.workers[type][server].load;
						this.reqmap[rid] = server;
						this.clientmap[rid] = clientid;
						send.call(this, [server, MDP.REQUEST, rid, data]);
					}
					return;
				}
			}
		}
	} while (false);

	console.log("|broker|do nothing", server, clientid, rid, data);
	send.call(this, [clientid, MDP.NOEXISTS, server, rid, data]);
}

function replyReq(server, workid, rid, data) {
	var clientid;
	// 首先检查是否是群发消息
	if (this.castmsg.hasOwnProperty(rid)) {
		if (this.castmsg[rid].hasOwnProperty(workid)) {
			if (this.clientmap.hasOwnProperty(rid)) {
				clientid = this.clientmap[rid];
				send.call(this, [clientid, MDP.REPLY, workid, rid, data]);
			}
			delete this.castmsg[rid][workid];
			allReceiveProc.call(this, rid);
		}
	} else if (this.reqmap.hasOwnProperty(rid)) {
		if (this.reqmap[rid] === workid) {
			if (this.clientmap.hasOwnProperty(rid)) {
				clientid = this.clientmap[rid];
				send.call(this, [clientid, MDP.REPLY, workid, rid, data]);
				delete this.clientmap[rid];
			}
			delete this.reqmap[rid];
		}
	}

	if (this.workers.hasOwnProperty(server)) {
		if (this.workers[server].hasOwnProperty(workid)) {
			this.workers[server][workid].liveness = HEALTH_LIVENESS;
			--this.workers[server][workid].load;
		}
	}
}

function dispatchReqs(workid) {
	var rid, clientid;
	for (rid in this.reqmap) {
		if (this.reqmap.hasOwnProperty(rid)) {
			if (this.reqmap[rid] === workid) {
				delete this.reqmap[rid];
				if (this.clientmap.hasOwnProperty(rid)) {
					clientid = this.clientmap[rid];
					// 这条消息可以优化，对于一个worker的离开其实只需要发送一条消息就可以了，request记录下req对应的worker表
					send.call(this, [clientid, MDP.DISCONNECT, workid, rid]);
					delete this.clientmap[rid];
				}
			}
		}
	}

	for (rid in this.castmsg) {
		if (this.castmsg.hasOwnProperty(rid)) {
			if (this.castmsg[rid].hasOwnProperty(workid)) {
				delete this.castmsg[rid][workid];
				allReceiveProc.call(this, rid);
			}
		}
	}
}

function checkWorkers() {
	var workid, server;
	for (server in this.workers) {
		if (this.workers.hasOwnProperty(server)) {
			for (workid in this.workers[server]) {
				if (this.workers[server].hasOwnProperty(workid)) {
					--this.workers[server][workid].liveness;
					if (this.workers[server][workid].liveness < 0) {
						console.log('one worker is kick out: ' + workid);
						dispatchReqs.call(this, workid);
						delete this.workers[server][workid];
					} else {
						send.call(this, [workid, MDP.HEARTBEAT]);
					}
				}
			}
		}
	}
}

function onMsg(args) {
	var identity = args[0].toString(),
		type = parseInt(args[1].toString(), 10), server, rid, data;
	if (type === MDP.REQUEST) {
		server = args[2].toString();
		rid = args[3].toString();
		data = args[4];
		routeReq.call(this, server, identity, rid, data);
	} else if (type === MDP.REPLY) {
		server = args[2].toString();
		rid = args[3].toString();
		data = args[4];
		replyReq.call(this, server, identity, rid, data);
	} else if (type === MDP.HEARTBEAT) {
		server = args[2].toString();
		// 生成新worker的映射关系
		if (!this.workers.hasOwnProperty(server)) {
			this.workers[server] = {};
		}
		// 初始化worker映射表中的标识
		if (!this.workers[server].hasOwnProperty(identity)) {
			this.workers[server][identity] = {};
			this.workers[server][identity].load = 0;
		}
		// 增加新的心跳值
		this.workers[server][identity].liveness = HEALTH_LIVENESS;
	}
}

var _broker = function(id, router, host) {
	this.router = router;
	this.host = host;
	this.id = id;

	this.workers = {};
	// workers =
	// {
	// 	conn: {
	// 		conn-1: {
	// 			liveness: 3
	// 			load: 2
	// 			}
	// 		}
	// 	}
	// }
	this.castmsg = {};
	// castmsg =
	// {
	// 	rid1: {
	// 		workid1: 0,
	// 		workid2: 0,
	// 	}
	// }
	this.reqmap = {};
	// reqmap =
	// {
	// 	rid1: workid1,
	// 	rid2: workid2,
	// }
	this.clientmap = {};
	// clientmap =
	// {
	// 	rid1: clientid
	// }

	EventEmitter.call(this);
};

util.inherits(_broker, EventEmitter);

module.exports = _broker;

var format_args = function(_args) {
	var args = [], i;
	for (i = 0; i < _args.length; ++i) {
		args.push(_args[i]);
	}
	return args;
};

_broker.prototype.open = function() {
	var that = this;

	this.socket = zmq.socket('router');
	this.socket.identity = 'broker-' + this.id;
	this.socket.on('message', function() {
		var args = format_args(arguments);
		setImmediate(function() {
			onMsg.call(that, args);
		});
	});
	this.socket.on('error', function(e) {
		that.emit('error', e);
	});

	this.socket.bindSync('tcp://' + this.host + ':' + this.router);

	// 维护broker和worker之间的心跳
	this.heartbeat = setInterval(checkWorkers.bind(this), HEARTBEAT_TIME);
};

_broker.prototype.close = function() {
	clearInterval(this.heartbeat);
	if (this.socket) {
		this.socket.close();
		delete this.socket;
	}
};
