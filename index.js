'use strict';

var constants = require('haraka-constants');
var ipaddr    = require('ipaddr.js');

exports.register = function () {
    var plugin = this;
    plugin.inherits('haraka-plugin-redis');

    plugin.register_hook('init_master',  'init_redis_plugin');
    plugin.register_hook('init_child',   'init_redis_plugin');

    plugin.load_limit_ini();

    if (plugin.cfg.concurrency) {
        plugin.register_hook('connect_init', 'incr_concurrency');
        plugin.register_hook('connect',      'check_concurrency');
        plugin.register_hook('disconnect',   'decr_concurrency');
    }

    if (plugin.cfg.errors) {
        ['helo','ehlo','mail','rcpt','data'].forEach(function (hook) {
            plugin.register_hook(hook, 'max_errors');
        });
    }

    if (plugin.cfg.recipients) {
        plugin.register_hook('rcpt', 'max_recipients');
    }

    if (plugin.cfg.unrecognized_commands) {
        plugin.register_hook('unrecognized_command', 'max_unrecognized_commands');
    }

    plugin.register_hook('connect', 'rate_rcpt_host');
    plugin.register_hook('connect', 'rate_conn');

    ['rcpt', 'rcpt_ok'].forEach(function (h) {
        plugin.register_hook(h,    'rate_rcpt_sender');
        plugin.register_hook(h,    'rate_rcpt_null');
        plugin.register_hook(h,    'rate_rcpt');
    });
};

exports.load_limit_ini = function () {
    var plugin = this;
    plugin.cfg = plugin.config.get('limit.ini', function () {
        plugin.load_limit_ini();
    });

    if (!plugin.cfg.concurrency) {   // no config file
        plugin.cfg.concurrency = {};
    }

    plugin.merge_redis_ini();
};

exports.shutdown = function () {
    if (this.db) this.db.quit();
}

exports.max_unrecognized_commands = function(next, connection, cmd) {
    var plugin = this;
    if (!plugin.cfg.unrecognized_commands) return next();

    connection.results.push(plugin, {unrec_cmds: cmd, emit: true});

    var max = parseFloat(plugin.cfg.unrecognized_commands.max);
    if (!max || isNaN(max)) return next();

    var uc = connection.results.get(plugin).unrec_cmds;
    if (!uc || !uc.length) return next();

    if (uc.length <= max) return next();

    connection.results.add(plugin, {fail: 'unrec_cmds.max'});
    plugin.penalize(connection, true, 'Too many unrecognized commands', next);
};

exports.max_errors = function (next, connection) {
    var plugin = this;
    if (!plugin.cfg.errors) return next();  // disabled in config

    var max = parseFloat(plugin.cfg.errors.max);
    if (!max || isNaN(max)) return next();

    if (connection.errors <= max) return next();

    connection.results.add(plugin, {fail: 'errors.max'});
    plugin.penalize(connection, true, 'Too many errors', next);
};

exports.max_recipients = function (next, connection, params) {
    var plugin = this;
    if (!plugin.cfg.recipients) return next(); // disabled in config

    var max = plugin.get_recipient_limit(connection);
    if (!max || isNaN(max)) return next();

    var c = connection.rcpt_count;
    var count = c.accept + c.tempfail + c.reject + 1;
    if (count <= max) return next();

    connection.results.add(plugin, { fail: 'recipients.max' });
    plugin.penalize(connection, false, 'Too many recipient attempts', next);
};

exports.get_recipient_limit = function (connection) {
    var plugin = this;

    if (connection.relaying && plugin.cfg.recipients.max_relaying) {
        return plugin.cfg.recipients.max_relaying;
    }

    var history_plugin = plugin.cfg.concurrency.history;
    if (!history_plugin) {
        return plugin.cfg.recipients.max;
    }

    var results = connection.results.get(history_plugin);
    if (!results) {
        connection.logerror(plugin, 'no ' + history_plugin + ' results,' +
               ' disabling history due to misconfiguration');
        delete plugin.cfg.recipients.history;
        return plugin.cfg.recipients.max;
    }

    if (results.history === undefined) {
        connection.logerror(plugin, 'no history from : ' + history_plugin);
        return plugin.cfg.recipients.max;
    }

    var history = parseFloat(results.history);
    connection.logdebug(plugin, 'history: ' + history);
    if (isNaN(history)) { history = 0; }

    if (history > 0) return plugin.cfg.recipients.history_good || 50;
    if (history < 0) return plugin.cfg.recipients.history_bad  || 2;
    return plugin.cfg.recipients.history_none || 15;
};

exports.incr_concurrency = function (next, connection) {
    var plugin = this;
    if (!plugin.cfg.concurrency) return next();

    var dbkey = plugin.get_key(connection);

    plugin.db.incr(dbkey, function (err, count) {
        if (err) {
            connection.results.add(plugin, { err: 'incr_concurrency:' + err });
            return next();
        }

        if (isNaN(count)) {
            connection.results.add(plugin, {err: 'incr_concurrency got isNaN'});
            return next();
        }

        connection.results.add(plugin, { concurrent_count: count });

        // repair negative concurrency counters
        if (count < 1) {
            connection.results.add(plugin, {
                msg: 'resetting concurrent ' + count + ' to 1'
            });
            plugin.db.set(dbkey, 1);
        }

        plugin.db.expire(dbkey, 120); // 2 minute lifetime
        next();
    });
};

exports.get_key = function (connection) {
    return 'concurrency|' + connection.remote.ip;
};

exports.check_concurrency = function (next, connection) {
    var plugin = this;

    var max = plugin.get_concurrency_limit(connection);
    if (!max || isNaN(max)) {
        connection.results.add(plugin, {err: "concurrency: no limit?!"});
        return next();
    }

    var count = parseInt(connection.results.get(plugin.name).concurrent_count);
    if (isNaN(count)) {
        connection.results.add(plugin, { err: 'concurrent.unset' });
        return next();
    }

    connection.results.add(plugin, { concurrent: count + '/' + max });

    if (count <= max) return next();

    connection.results.add(plugin, { fail: 'concurrency.max' });

    plugin.penalize(connection, true, 'Too many concurrent connections', next);
};

exports.get_concurrency_limit = function (connection) {
    var plugin = this;

    var history_plugin = plugin.cfg.concurrency.history;
    if (!history_plugin) {
        return plugin.cfg.concurrency.max;
    }

    var results = connection.results.get(history_plugin);
    if (!results) {
        connection.logerror(plugin, 'no ' + history_plugin + ' results,' +
               ' disabling history due to misconfiguration');
        delete plugin.cfg.concurrency.history;
        return plugin.cfg.concurrency.max;
    }

    if (results.history === undefined) {
        connection.loginfo(plugin, 'no IP history from : ' + history_plugin);
        return plugin.cfg.concurrency.max;
    }

    var history = parseFloat(results.history);
    connection.logdebug(plugin, 'history: ' + history);
    if (isNaN(history)) { history = 0; }

    if (history < 0) { return plugin.cfg.concurrency.history_bad  || 1; }
    if (history > 0) { return plugin.cfg.concurrency.history_good || 5; }
    return plugin.cfg.concurrency.history_none || 3;
};

exports.penalize = function (connection, disconnect, msg, next) {
    var plugin = this;
    var code = disconnect ? constants.DENYSOFTDISCONNECT : constants.DENYSOFT;

    if (!plugin.cfg.main.tarpit_delay) {
        return next(code, msg);
    }

    var delay = plugin.cfg.main.tarpit_delay;
    connection.loginfo(plugin, 'tarpitting for ' + delay + 's');

    setTimeout(function () {
        if (!connection) return;
        next(code, msg);
    }, delay * 1000);
}

exports.decr_concurrency = function (next, connection) {
    var plugin = this;
    if (!plugin.cfg.concurrency) return next();

    var dbkey = plugin.get_key(connection);
    plugin.db.incrby(dbkey, -1, function (err, concurrent) {
        if (err) connection.results.add(plugin, { err: 'decr_concurrency:' + err })
        return next();
    });
};

exports.lookup_host_key = function (type, remote, cb) {
    var plugin = this;
    if (!plugin.cfg[type]) {
        return cb(new Error(type + ': not configured'));
    }

    try {
        var ip = ipaddr.parse(remote.ip);
        if (ip.kind === 'ipv6') {
            ip = ipaddr.toNormalizedString();
        }
        else {
            ip = ip.toString();
        }
    }
    catch (err) {
        return cb(err);
    }

    var ip_array = ((ip.kind === 'ipv6') ? ip.split(':') : ip.split('.'));
    while (ip_array.length) {
        var part = ((ip.kind === 'ipv6') ? ip_array.join(':') : ip_array.join('.'));
        if (plugin.cfg[type][part] || plugin.cfg[type][part] === 0) {
            return cb(null, part, plugin.cfg[type][part]);
        }
        ip_array.pop();
    }

    // rDNS
    if (remote.host) {
        var rdns_array = remote.host.toLowerCase().split('.');
        while (rdns_array.length) {
            var part2 = rdns_array.join('.');
            if (plugin.cfg[type][part2] || plugin.cfg[type][part2] === 0) {
                return cb(null, part2, plugin.cfg[type][part2]);
            }
            rdns_array.pop();
        }
    }

    // Custom Default
    if (plugin.cfg[type].default) {
        return cb(null, ip, plugin.cfg[type].default);
    }

    // Default 0 = unlimited
    return cb(null, ip, 0);
};

exports.get_mail_key = function (type, mail, cb) {
    var plugin = this;
    if (!plugin.cfg[type] || !mail) return cb();

    // Full e-mail address (e.g. smf@fsl.com)
    var email = mail.address();
    if (plugin.cfg[type][email] || plugin.cfg[type][email] === 0) {
        return cb(email, plugin.cfg[type][email]);
    }

    // RHS parts e.g. host.sub.sub.domain.com
    if (mail.host) {
        var rhs_split = mail.host.toLowerCase().split('.');
        while (rhs_split.length) {
            var part = rhs_split.join('.');
            if (plugin.cfg[type][part] || plugin.cfg[type][part] === 0) {
                return cb(part, plugin.cfg[type][part]);
            }
            rhs_split.pop();
        }
    }

    // Custom Default
    if (plugin.cfg[type].default) {
        return cb(email, plugin.cfg[type].default);
    }

    // Default 0 = unlimited
    return cb(email, 0);
};

function getTTL (value) {

    var match = /^(\d+)(?:\/(\d+)(\S)?)?$/.exec(value);
    if (!match) return;

    var qty = match[2];
    var units = match[3];

    var ttl = qty ? qty : 60;  // Default 60s
    if (!units) return ttl;

    // Unit
    switch (units.toLowerCase()) {
        case 's':               // Default is seconds
            break;
        case 'm':
            ttl *= 60;          // minutes
            break;
        case 'h':
            ttl *= (60*60);     // hours
            break;
        case 'd':
            ttl *= (60*60*24);  // days
            break;
        default:
            return;
    }
    return ttl;
}

function getLimit (value) {
    var match = /^([\d]+)/.exec(value);
    if (!match) return;
    return match[1];
}

exports.rate_limit = function (connection, key, value, cb) {
    var plugin = this;

    if (value === 0) {     // Limit disabled for this host
        connection.loginfo(this, 'rate limit disabled for: ' + key);
        return cb(null, false);
    }

    // CAUTION: !value would match that 0 value -^
    if (!key || !value) return cb();

    var limit = getLimit(value);
    var ttl = getTTL(value);

    if (!limit || ! ttl) {
        return cb(new Error('syntax error: key=' + key + ' value=' + value));
    }

    connection.logdebug(plugin, 'key=' + key + ' limit=' + limit + ' ttl=' + ttl);

    plugin.db.incr(key, function (err, newval) {
        if (err) return cb(err);

        if (newval === 1) plugin.db.expire(key, ttl);
        cb(err, parseInt(newval) > parseInt(limit)); // boolean true/false
    });
};

exports.rate_rcpt_host = function (next, connection) {
    var plugin = this;

    if (!plugin.cfg.rate_rcpt_host) return next();

    plugin.lookup_host_key('rate_rcpt_host', connection.remote, function (err, key, value) {
        if (err) {
            connection.results.add(plugin, { err: 'rate_rcpt_host:' + err });
            return next();
        }

        if (!key || !value) return next();

        var match = /^(\d+)/.exec(value);
        var limit = match[0];
        if (!limit) return next();

        plugin.db.get('rate_rcpt_host:' + key, function (err2, result) {
            if (err2) {
                connection.results.add(plugin, { err: 'rate_rcpt_host:' + err2 });
                return next();
            }

            if (!result) return next();
            connection.results.add(plugin, {
                rate_rcpt_host: key + ':' + result + '/' + limit
            });

            if (result <= limit) return next();
            connection.results.add(plugin, { fail: 'rate_rcpt_host' });
            plugin.penalize(connection, false, 'connection rate limit exceeded', next);
        });
    });
}

exports.rate_conn = function (next, connection) {
    var plugin = this;

    plugin.lookup_host_key('rate_conn', connection.remote, function (err, key, value) {
        if (err) {
            connection.results.add(plugin, { err: 'rate_conn:' + err });
            return next();
        }

        if (value === 0) return next(); // limits disabled for host
        if (!key || !value) return next();

        var limit = getLimit(value);
        var ttl = getTTL(value);
        if (!limit || ! ttl) {
            connection.results.add(plugin, { err: 'rate_conn:syntax:' + value });
            return next();
        }

        plugin.db.incr('rate_conn:' + key, function (err2, newval) {
            if (err2) {
                connection.results.add(plugin, { err: 'rate_conn:' + err });
                return next();
            }

            if (newval === 1) plugin.db.expire('rate_conn:' + key, ttl);

            connection.results.add(plugin, { rate_conn: newval + '/' + limit});

            if (parseInt(newval) <= parseInt(limit)) return next();

            connection.results.add(plugin, { fail: 'rate_conn' });

            plugin.penalize(connection, true, 'connection rate limit exceeded', next);
        });
    });
};

exports.rate_rcpt_sender = function (next, connection, params) {
    var plugin = this;

    plugin.get_mail_key('rate_rcpt_sender', connection.transaction.mail_from, function (key, value) {

        plugin.rate_limit(connection, 'rate_rcpt_sender' + ':' + key, value, function (err, over) {
            if (err) {
                connection.results.add(plugin, { err: 'rate_rcpt_sender:' + err });
                return next();
            }

            connection.results.add(plugin, { rate_rcpt_sender: value });

            if (!over) return next();

            connection.results.add(plugin, { fail: 'rate_rcpt_sender' });
            plugin.penalize(connection, false, 'rcpt rate limit exceeded', next);
        });
    });
};

exports.rate_rcpt_null = function (next, connection, params) {
    var plugin = this;

    if (Array.isArray(params)) params = params[0];
    if (params.user) return next();

    // Message from the null sender
    plugin.get_mail_key('rate_rcpt_null', params, function (key, value) {

        plugin.rate_limit(connection, 'rate_rcpt_null' + ':' + key, value, function (err2, over) {
            if (err2) {
                connection.results.add(plugin, { err: 'rate_rcpt_null:' + err2 });
                return next();
            }

            connection.results.add(plugin, { rate_rcpt_null: value });

            if (!over) return next();

            connection.results.add(plugin, { fail: 'rate_rcpt_null' });
            plugin.penalize(connection, false, 'null recip rate limit', next);
        });
    });
};

exports.rate_rcpt = function (next, connection, params) {
    var plugin = this;
    if (Array.isArray(params)) params = params[0];
    plugin.get_mail_key('rate_rcpt', params, function (key, value) {

        plugin.rate_limit(connection, 'rate_rcpt' + ':' + key, value, function (err2, over) {
            if (err2) {
                connection.results.add(plugin, { err: 'rate_rcpt:' + err2 });
                return next();
            }

            connection.results.add(plugin, { rate_rcpt: value });

            if (!over) return next();

            connection.results.add(plugin, { fail: 'rate_rcpt' });
            plugin.penalize(connection, false, 'rate limit exceeded', next);
        });
    });
};
