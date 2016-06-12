var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-q2a]';

(function(Exporter) {

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'root',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || 'q2a'
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		callback(null, Exporter.config());
	};

	Exporter.getUsers = function(callback) {
		Exporter.log('getUsers');
		return Exporter.getPaginatedUsers(0, -1, callback);
	};
	Exporter.getPaginatedUsers = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
			+'auth_user.id as _uid, '
			+'auth_user.email as _email, '
			+'auth_user.username as _username, '
			+'auth_user.date_joined as _joindate, '
			+'auth_user.last_login as _lastonline '
			+ 'FROM auth_user '
			+ 'RIGHT JOIN ' + prefix + 'userpoints as up '
			+ 'ON up.userid=auth_user.id '
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._description = row._description || 'No decsciption available';
					row._joindate = +new Date(row._joindate) || startms;
					row._lastonline = +new Date(row._lastonline) || startms;

					//if (row._uid < 25000)
						map[row._uid] = row;
				});

				callback(null, map);
			});
	};

	Exporter.getGroups = function(callback) {
		Exporter.log('getGroups');
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		callback(null, {});
	};

	Exporter.getCategories = function(callback) {
		Exporter.log('getCategories');
		return Exporter.getPaginatedCategories(0, -1, callback);
	};
	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
			+ prefix + 'categories.categoryid as _cid, '
			+ prefix + 'categories.title as _name, '
			+ prefix + 'categories.content as _description, '
			+ prefix + 'categories.position as _order, '
			+ prefix + 'categories.backpath as _path, '
			+ prefix + 'categories.parentid as _parentCid '
			+ 'FROM ' + prefix + 'categories '
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._description = row._description || 'No decsciption available';
					row._timestamp = +new Date(row._timestamp) || startms;

					map[row._cid] = row;
				});

				callback(null, map);
			});
	};

	Exporter.getTopics = function(callback) {
		Exporter.log('getTopics');
		return Exporter.getPaginatedTopics(0, -1, callback);
	};
	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
			+ prefix + 'posts.postid as _tid, '
			+ prefix + 'posts.userid as _uid, '
			+ prefix + 'posts.categoryid as _cid, '
			+ prefix + 'posts.createip as _ip, '                    // need do some conversion
			+ prefix + 'posts.title as _title, '
			+ prefix + 'posts.content as _content, '
			+ prefix + 'posts.created as _timestamp, '              // need do some conversion
			+ prefix + 'posts.views as _viewcount, '
			+ prefix + 'posts.tags as _tags, '                      // need do some conversion
			+ prefix + 'posts.type=\'Q_HIDDEN\' as _deleted, '
			+ prefix + 'posts.updated as _edited, '                 // need do some conversion
			+ 'u.email as _uemail '
			+ 'FROM ' + prefix + 'posts '
			+ 'LEFT JOIN auth_user as u '
			+ 'ON u.id=' + prefix + 'posts.userid '
			+ 'WHERE type=\'Q\' or type=\'Q_HIDDEN\''
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._timestamp = +new Date(row._timestamp) || startms;
					row._edited = +new Date(row._edited) || startms;
					row._tags = row._tags.split(',');

					//if (row._uid < 25000)
						map[row._tid] = row;
				});

				callback(null, map);
			});
	};

	Exporter.getPosts = function(callback) {
		Exporter.log('getPosts');
		return Exporter.getPaginatedPosts(0, -1, callback);
	};

	var getAnswers = function(start, limit, callback) {
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
			+ prefix + 'posts.postid as _pid, '
			+ prefix + 'posts.parentid as _tid, '
			+ prefix + 'posts.content as _content, '
			+ prefix + 'posts.userid as _uid, '
			+ prefix + 'posts.created as _timestamp, '
			+ prefix + 'posts.createip as _ip, '
			+ prefix + 'posts.updated as _edited, '
			+ 'u.email as _uemail '
			+ 'FROM ' + prefix + 'posts '
			+ 'LEFT JOIN auth_user as u '
			+ 'ON u.id=' + prefix + 'posts.userid '
			+ 'WHERE type=\'A\' or type=\'A_HIDDEN\''
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._timestamp = +new Date(row._timestamp) || startms;
					row._edited = +new Date(row._edited) || startms;

					//if (row._uid < 25000)
						map[row._pid] = row;
				});

				callback(null, map);
			});
	};

	var getComments = function(start, limit, callback) {
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
			+ 'p1.postid as _pid, '

				// old topicid depend on its parent, whether its parent is a Question or Answer
			+ 'p1.parentid as _tid, '

			+ 'p1.content as _content, '
			+ 'p1.userid as _uid, '
				// _toPid also depends on whether its parent is a Question or Answer
				// + 'p1.userid as _toPid, '
			+ 'p1.created as _timestamp, '
			+ 'p1.createip as _ip, '
			+ 'p1.updated as _edited, '
			+ 'p2.type as _parent_type, '
			+ 'p2.parentid as _parent_parent '
			+ 'FROM ' + prefix + 'posts as p1 '
			+ 'LEFT JOIN ' + prefix + 'posts as p2 '
			+ 'ON p1.parentid=p2.postid '
			+ 'WHERE p1.type in (\'C\', \'C_HIDDEN\')'
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._timestamp = +new Date(row._timestamp) || startms;
					row._edited = +new Date(row._edited) || startms;

					if (row._parent_type == 'A' || row._parent_type == 'A_HIDDEN') {
						row._toPid = row._tid;
						row._tid = row._parent_parent;
					}
					//if (row._uid < 25000)
						map[row._pid] = row;
				});

				callback(null, map);
			});
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		async.series({
			answer: function (next) {
				getAnswers(start, limit, next);
			},
			comment: function(next) {
				getComments(start, limit, next);
			}
		}, function(err, result) {
			if (err) {
				return callback(err);
			}
			callback(null, _.extend(result['answer'], result['comment']));
		});
	};

	Exporter.getVotes = function(callback) {
		Exporter.log('getVotes');
		return Exporter.getPaginatedVotes(0, -1, callback);
	};
	Exporter.getPaginatedVotes = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				// since qa_uservotes has no voteid at all, so we need to generate it ourselves
				// + 'v.voteid as _vid'
			+ 'v.userid as _uid, '
			+ 'v.postid as _pid, '
			+ 'v.vote as _action, '
			+ 'p.type as _post_type, '
			+ 'u.email as _uemail'
			+ 'FROM ' + prefix + 'uservotes as v '
			+ 'LEFT JOIN ' + prefix + 'posts as p, auth_user as u '
			+ 'ON v.postid=p.postid and u.id=v.userid'
			+ 'WHERE v.vote!=0 '
			+  (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}

				//normalize here
				var map = {};
				var vid = 1;
				rows.forEach(function(row) {
					if (row._post_type == 'Q' || row._parent_type == 'Q_HIDDEN') {
						row._tid = row._pid;
						row._pid = null;
						row._vid = vid++;
					}

					//if (row._uid < 25000)
						map[row._vid] = row;
				});

				callback(null, map);
			});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
			function(next) {
				Exporter.getVotes(next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getPaginatedUsers(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);