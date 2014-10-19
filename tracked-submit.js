var assert = require('assert')
var debug = require('debug')('ripple-client:tracked-submit')

function wrapError(inner, msg, name) {
    var err = new Error(msg)
    err.inner = inner
    err.name = name
    return err
}

module.exports = function(ripple, tx, cb) {
    // TODO: Wait for open?
    function rippleOpen() {
        debug('subscribing to transactions')

        ripple.request('subscribe', { streams: ['transactions'] }, function(err) {
            if (err) {
                var wrappedErr = wrapError(err, 'Failed to subscribe', 'SubscribeFailed')
                return finish(err)
            }

            debug('subscribed to transaction stream')

            if (!submitted) {
                submitted = true
                submit()
            }
        })
    }

    function rippleClose() {
        if (submitted) {
            debug('ripple closed while waiting for tx')
        }
    }

    function rippleTransaction(tx) {
        if (!submitResult) return

        var inner = tx.transaction
        var meta = tx.meta

        if (inner.hash != submitResult.tx_json.hash) return

        assert.equal(tx.status, 'closed')
        assert.equal(tx.validated, true)
        assert.equal(tx.type, 'transaction')
        assert.equal(inner.TransactionType, 'Payment')

        if (meta.TransactionResult == 'tesSUCCESS') {
            return finish(null, submitResult.tx_json.hash)
        }

        var err = new Error('TransactionResult is not tesSUCCESS')
        err.name = 'TransactionFailed'
        err.transactionResult = meta.TransactionResult
        finish(err)
    }

    function submit() {
        ripple.request('submit', tx, function(err, res) {
            if (err) {
                finish(wrapError(err, 'Failed to submit payment', 'SubmitFailed'))
                return
            }

            submitResult = res

            if (res.engine_result != 'tesSUCCESS') {
                err = new Error(res.engine_result_message)
                err.name = 'SubmitFailed'
                err.engineResult = res.engine_result
                finish(err)
                return
            }

            debug('request submitted. hash %s', res.tx_json.hash)
        })
    }

    function finish(err, res) {
        debug('cleaning up')
        ripple.removeListener('open', rippleOpen)
        ripple.removeListener('close', rippleClose)
        ripple.removeListener('transaction', rippleTransaction)
        cb(err, res)
    }

    var submitResult, submitted

    assert.equal(ripple.opts.allTransactions, true, 'allTransactions must be set on the Ripple client')

    ripple.on('open', rippleOpen)
    ripple.on('close', rippleClose)
    ripple.on('transaction', rippleTransaction)
    ripple.connected && rippleOpen()
}
