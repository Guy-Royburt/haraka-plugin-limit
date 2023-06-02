'use strict';

const constants = require('haraka-constants');
const ipaddr = require('ipaddr.js');

exports.register = function () {
  this.inherits('haraka-plugin-redis');

  this.load_limit_ini();
  let needs_redis = 0;

  if (this.cfg.concurrency.enabled) {
    needs_redis++;
    this.register_hook('connect_init', 'conn_concur_incr');
    this.register_hook('connect', 'check_concurrency');
    this.register_hook('disconnect', 'conn_concur_decr');
  }

  if (this.cfg.errors.enabled) {
    ['helo', 'ehlo', 'mail', 'rcpt', 'data'].forEach((hook) => {
      this.register_hook(hook, 'max_errors');
    });
  }

  if (this.cfg.recipients.enabled) {
    this.register_hook('rcpt', 'max_recipients');
  }

  if (this.cfg.unrecognized_commands.enabled) {
    this.register_hook('unrecognized_command', 'max_unrecognized_commands');
  }

  if (this.cfg.rate_conn.enabled) {
    needs_redis++;
    this.register_hook('connect_init', 'rate_conn_incr');
    this.register_hook('connect', 'rate_conn_enforce');
  }
  if (this.cfg.rate_rcpt_host.enabled) {
    needs_redis++;
    this.register_hook('connect', 'rate_rcpt_host_enforce');
    this.register_hook('rcpt', 'rate_rcpt_host_incr');
  }
  if (this.cfg.rate_rcpt_sender.enabled) {
    needs_redis++;
    this.register_hook('rcpt', 'rate_rcpt_sender');
  }
  if (this.cfg.rate_rcpt_null.enabled) {
    needs_redis++;
    this.register_hook('rcpt', 'rate_rcpt_null');
  }
  if (this.cfg.rate_rcpt.enabled) {
    needs_redis++;
    this.register_hook('rcpt', 'rate_rcpt');
  }

  if (this.cfg.outbound.enabled) {
    needs_redis++;
    this.register_hook('send_email', 'outbound_increment');
    this.register_hook('delivered', 'outbound_decrement');
    this.register_hook('deferred', 'outbound_decrement');
    this.register_hook('bounce', 'outbound_decrement');
  }

  if (needs_redis) {
    this.register_hook('init_master', 'init_redis_plugin');
    this.register_hook('init_child', 'init_redis_plugin');
  }
};

exports.load_limit_ini = function () {
  const plugin = this;
  plugin.cfg = plugin.config.get('limit.ini', {
    booleans: [
      '-outbound.enabled',
      '-recipients.enabled',
      '-unrecognized_commands.enabled',
      '-errors.enabled',
      '-rate_conn.enabled',
      '-rate_rcpt.enabled',
      '-rate_rcpt_host.enabled',
      '-rate_rcpt_sender.enabled',
      '-rate_rcpt_null.enabled',
    ],
  }, function () {
    plugin.load_limit_ini();
  });

  if (!this.cfg.concurrency) {
    // no config file
    this.cfg.concurrency = {};
  }

  this.merge_redis_ini();
};

exports.shutdown = function () {
  if (this.db) this.db.quit();
};

exports.max_unrecognized_commands = function (next, connection, cmd) {
    if (!this.cfg.unrecognized_commands) return next();
  
    connection.results.push(this, { unrec_cmds: cmd, emit: true });
  
    const max = parseFloat(this.cfg.unrecognized_commands.max);
    if (!max || isNaN(max)) return next();
  
    const uc = connection.results.get(this).un; // Complete the line with the necessary code
  
    next();
  };
  
