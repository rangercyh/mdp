/*
worker
主动发送心跳给broker
如果发现broker生命到期了，就销毁数据和socket重新不断尝试建立连接
发送消息：[HEARTBEAT, server] [REPLY, server, rid, data]
接收消息：[REQUEST, rid, data] [HEARTBEAT]
*/
var zmq = require('zmq');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var MDP = {
	REQUEST: 1,
	REPLY: 2,
	HEARTBEAT: 3,
	DISCONNECT: 4,
	DONE: 5,
	NOEXISTS: 6
};
var HEARTBEAT_TIME = 2500;
var BROKER_LIVENESS = 3;

var _worker = function(router, host, id, server) {
	this.host = host;
	this.id = id;
	this.server = server;
	this.router = router;
	this._hbtime = 0;
	EventEmitter.call(this);
};
util.inherits(_worker, EventEmitter);

module.exports = _worker;

function send(msg) {
	if (!this.socket) {
		return;
	}
	this._hbtime = Date.now();
	this.socket.send(msg);
}

_worker.prototype.open = function() {
	this.connectBroker();
};

_worker.prototype.close = function() {
	if (this.heartbeat) {
		clearInterval(this.heartbeat);
	}
	if (this.socket) {
		this.socket.close();
		delete this.socket;
	}
};

_worker.prototype.fuckHeartbeat = function() {
	if (this._hbtime) {
		if ((Date.now() - this._hbtime) < HEARTBEAT_TIME) {
			return;
		}
	}
	send.call(this, [MDP.HEARTBEAT, this.server]);
};

function checkHealth() {
	--this.liveness;
	if (this.liveness < 0) {
		this.connectBroker();
		return;
	}

	this.fuckHeartbeat();
}

function onMsg(args) {
	var type = parseInt(args[0].toString(), 10), msg, data;
	if (type === MDP.REQUEST) {
		msg = args[2];
		data = JSON.parse(msg);
		if (data) {
			this.emit('data', data);
		}
	}
	this.liveness = BROKER_LIVENESS;
}

var format_args = function(_args) {
	var args = [], i;
	for (i = 0; i < _args.length; ++i) {
		args.push(_args[i]);
	}
	return args;
};

_worker.prototype.connectBroker = function() {
	var that = this;

	this.close();

	this.socket = zmq.socket('dealer');
	this.socket.identity = this.id;
	this.socket.on('message', function() {
		var args = format_args(arguments);
		onMsg.call(that, args);
	});
	this.socket.on('error', function(e) {
		that.emit('error', e);
	});

	this.socket.connect('tcp://' + that.host + ':' + that.router);

	this.fuckHeartbeat();

	this.liveness = BROKER_LIVENESS;

	this.heartbeat = setInterval(checkHealth.bind(this), HEARTBEAT_TIME);
};

_worker.prototype.reply = function(rid, msg) {
	var buffer = JSON.stringify(msg);
	if (buffer) {
		send.call(this, [MDP.REPLY, this.server, rid, buffer]);
	}
};

