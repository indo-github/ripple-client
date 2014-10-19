var MAX_TRANSACTIONS = 200
var _ = require('lodash')
var EventEmitter = require('events').EventEmitter
var util = require('util')
var debug = require('debug')('ripple-client:acctmon')
var async = require('async')
var assert = require('assert')

module.exports = exports = function(opts) {
    _.bindAll(this)
    this.opts = opts
    this.ripple = opts.ripple
    this.ripple.on('open', this.rippleOpen)
    this.ripple.on('close', this.rippleClose)
    this.ripple.on('transaction', this.rippleTransaction)
    this.ripple.on('ledgerclosed', this.rippleLedgerClosed)
    this.internalLedger = opts.ledgerIndex || 0
    this.accounts = {}
}

util.inherits(exports, EventEmitter)

exports.prototype.rippleOpen = function() {
    this.live = false
    this.processedHashes = []

    async.series([
        // Attach existing subscriptions
        function(cb) {
            debug('attaching existing subscriptions...')
            async.each(Object.keys(this.accounts), function(account, cb) {
                debug('subscribing to %s', account)
                this.subscribeToAccount(account, cb)
            }.bind(this), cb)
        }.bind(this),

        // Catch up from internal ledger to the closed one
        function(cb) {
            debug('catching up from ledger #%s...', this.internalLedger + 1)
            async.each(Object.keys(this.accounts), function(account, cb) {
                this.catchupAccount(account, this.internalLedger + 1, cb)
            }.bind(this), cb)
        }.bind(this),

        // Subscribe to ledger closes
        function(cb) {
            debug('subscribing to ledger close...')
            this.subscribeToLedgerClose(function(err) {
                if (err) return cb(err)
                debug('subscribed to ledger close')
                cb()
            })
        }.bind(this)
    ], function(err) {
        if (err) {
            if (err.message == 'Not synced to Ripple network.') {
                debug('node is not synced to the network. retry in 5 sec')
                return setTimeout(this.rippleOpen.bind(this), 5e3)
            }

            var wrappedErr = new Error('Initialization failed: ' + err.message)
            wrappedErr.inner = err
            return this.emit('error', wrappedErr)
        }
        this.live = true
        this.processedHashes = null
        debug('caught up and live')
    }.bind(this))
}

exports.prototype.rippleLedgerClosed = function(message) {
    assert.equal(this.live, true)

    debug('ledger %s closed', message.ledger_index)

    this.internalLedger = message.ledger_index
    this.emit('ledgerclosed', message.ledger_index)

    Object.keys(this.accounts).forEach(function(account) {
        this.catchupAccount(account, message.ledger_index, function(err) {
            if (!err) return
            console.error('failed to catch up %s from closed ledger %s: %s',
                account, message.ledger_index, err.message)
        }.bind(this))
    }.bind(this))
}

exports.prototype.catchupAccount = function(account, from, cb) {
    var that = this

    function next(marker) {
        var options = {
            account: account,
            ledger_index_min: from,
            ledger_index_max: -1,
            marker: marker || null,
            limit: MAX_TRANSACTIONS
        }

        if (marker !== undefined) {
            options.marker = marker
        }

        that.ripple.request('account_tx', options, function(err, res) {
            if (err) return cb(err)
            assert(res.transactions)
            res.transactions.forEach(function(tx) {
                if (tx.meta.TransactionResult != 'tesSUCCESS') return
                that.processTransaction(tx.tx, tx.meta)
            }.bind(that))
            if (!res.marker) return cb()
            next(res.marker)
        }.bind(that))
    }

    next()
}

exports.prototype.subscribeToLedgerClose = function(cb) {
    this.ripple.request('subscribe', {
        streams: ['ledger']
    }, cb)
}

exports.prototype.rippleClose = function() {
    debug('disconnected from ripple')
}

exports.prototype.subscribeToAccount = function(account, cb) {
    this.ripple.request('subscribe', {
        accounts: [account]
    }, cb)
}

exports.prototype.account = function(account, cb) {
    debug('adding subscription to account %s', account)
    var item = this.accounts[account]
    if (!item) {
        item = (this.accounts[account] = [])
        if (this.ripple.connected) {
            this.subscribeToAccount(account)
        }
    }
    item.push(cb)
}

exports.prototype.rippleTransaction = function(tx) {
    this.processTransaction(tx.transaction, tx.meta)
}

exports.prototype.processTransaction = function(inner, meta) {
    if (inner.TransactionType != 'Payment') {
        return debug('Ignoring tx type %s', inner.TransactionType)
    }

    if (!this.live) {
        // Has the transaction already been processed by catch-up?
        if (~this.processedHashes.indexOf(tx.hash)) return

        this.processedHashes.push(tx.hash)
    }

    _.each(this.accounts, function(subs, account) {
        if (account != inner.Destination) return
        subs.forEach(function(sub) {
            sub(inner, meta)
        })
    })
}
