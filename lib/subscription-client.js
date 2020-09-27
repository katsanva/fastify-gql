'use strict'

const WebSocket = require('ws')
const hasher = require('node-object-hash')
const errorHack = require('./gateway/lifehack')
const {
  GQL_CONNECTION_INIT,
  GQL_CONNECTION_ACK,
  GQL_CONNECTION_ERROR,
  GQL_CONNECTION_KEEP_ALIVE,
  GQL_START,
  GQL_DATA,
  GQL_ERROR,
  GQL_COMPLETE,
  GQL_STOP,
  GRAPHQL_WS
} = require('./subscription-protocol')
const HEARTBEAT_INTERVAL = 20 * 1000
const { hash } = hasher({ sort: true, coerce: true })

function heartbeat (socket) {
  if (!socket) {
    return
  }

  socket.isAlive = true
}

class SubscriptionClient {
  constructor (uri, config) {
    this.uri = uri
    this.socket = null
    this.operationId = 0
    this.ready = false
    this.operations = new Map()
    this.operationsCount = {}
    const {
      protocols = [],
      reconnect,
      maxReconnectAttempts = Infinity,
      serviceName,
      connectionCallback,
      failedConnectionCallback,
      failedReconnectCallback,
      connectionInitPayload
    } = config

    this.protocols = [GRAPHQL_WS, ...protocols]
    this.tryReconnect = reconnect
    this.maxReconnectAttempts = maxReconnectAttempts
    this.serviceName = serviceName
    this.reconnectAttempts = 0
    this.connectionCallback = connectionCallback
    this.failedConnectionCallback = failedConnectionCallback
    this.failedReconnectCallback = failedReconnectCallback
    this.connectionInitPayload = connectionInitPayload

    this.connect()
    errorHack.on('reconnect', () => {
      setImmediate(() => {
        this.close(true, false)
      })
    })
  }

  connect () {
    this.socket = new WebSocket(this.uri, this.protocols)

    this.socket.onopen = async () => {
      /* istanbul ignore else */
      if (this.socket && this.socket.readyState === WebSocket.OPEN) {
        try {
          const payload =
            typeof this.connectionInitPayload === 'function'
              ? await this.connectionInitPayload()
              : this.connectionInitPayload
          this.sendMessage(null, GQL_CONNECTION_INIT, payload)
        } catch (err) {
          this.close(this.tryReconnect, false)
        }
      }

      if (!this.socket) {
        return
      }

      this.socket.isAlive = true
      const keepaliveInterval = setInterval(() => {
        if (!this.socket) {
          clearInterval(keepaliveInterval)
          return
        }
        if (!this.socket || !this.socket.isAlive) {
          this.close(true, false)
          return
        }
        this.socket.isAlive = false
        this.socket.ping(null, true)
      }, HEARTBEAT_INTERVAL).unref()
    }

    this.socket.on('pong', () => {
      heartbeat(this.socket)
    })

    this.socket.onclose = () => {
      if (!this.closedByUser) {
        this.close(this.tryReconnect, false)
      }
    }

    this.socket.onerror = (e) => {
      console.log('Socket error', this.uri, e.message)
    }

    this.socket.onmessage = async ({ data }) => {
      await this.handleMessage(data)
    }
  }

  close (tryReconnect, closedByUser = true) {
    this.closedByUser = closedByUser
    this.ready = false

    if (this.socket !== null) {
      if (closedByUser) {
        this.unsubscribeAll()
      }

      this.socket.close()
      this.socket = null
      this.reconnecting = false

      if (tryReconnect) {
        this.operations.forEach((op) => {
          op.started = false
        })

        this.reconnect()
      }
    }
  }

  getReconnectDelay () {
    const delayMs = 100 * Math.pow(2, this.reconnectAttempts)

    return Math.min(delayMs, 10000)
  }

  reconnect () {
    if (
      this.reconnecting ||
      this.reconnectAttempts > this.maxReconnectAttempts
    ) {
      return this.failedReconnectCallback && this.failedReconnectCallback()
    }

    this.reconnectAttempts++
    this.reconnecting = true

    const delay = this.getReconnectDelay()

    this.reconnectTimeoutId = setTimeout(() => {
      this.connect()
    }, delay)
  }

  /**
   *
   * @param {{eids: Set}} operation
   * @param {*} eid
   * @param {*} forceUnsubscribe
   */
  unsubscribe (operation, eid, forceUnsubscribe) {
    const operationId = operation.oid
    let count = this.operationsCount[operationId]
    count--

    operation.eids.delete(eid)

    if (count === 0 || forceUnsubscribe) {
      this.sendMessage(eid, GQL_STOP, null)
      this.operationsCount[operationId] = 0
      this.operations.delete(operationId)
    } else {
      this.operationsCount[operationId] = count
    }
  }

  unsubscribeAll () {
    for (const operation of this.operations.values()) {
      for (const eid of operation.eids) this.unsubscribe(operation, eid, true)
    }
  }

  sendMessage (eid, type, payload = {}, extensions) {
    try {
      this.socket.send(
        JSON.stringify({
          id: eid,
          type,
          payload,
          extensions
        })
      )
    } catch (err) {
      this.close(true, false)
    }
  }

  async handleMessage (message) {
    let data
    let eid
    let operationId
    let operation

    try {
      data = JSON.parse(message)
      eid = data.id
    } catch (e) {
      /* istanbul ignore next */
      throw new Error(
        `Invalid message received: "${message}" Message must be JSON parsable.`
      )
    }

    if (eid) {
      operationId = this.oidFromEid(eid)
      operation = this.operations.get(operationId)
    }

    switch (data.type) {
      case GQL_CONNECTION_ACK:
        this.reconnecting = false
        this.ready = true
        this.reconnectAttempts = 0

        for (const operation of this.operations.values()) {
          this.startOperation(operation)
        }

        if (this.connectionCallback) {
          this.connectionCallback()
        }

        break
      case GQL_DATA:
        /* istanbul ignore else */
        if (operation) {
          operation.handler(data.payload.data)
        }
        break
      case GQL_ERROR:
        /* istanbul ignore else */
        if (operation) {
          operation.handler(null)
          this.operations.delete(operationId)
          this.sendMessage(eid, GQL_ERROR, data.payload)
        }
        break
      case GQL_COMPLETE:
        /* istanbul ignore else */
        if (operation && operation.eids.has(eid)) {
          operation.handler(null)
          this.operations.delete(operationId)
        }

        break
      case GQL_CONNECTION_ERROR:
        this.close(this.tryReconnect, false)
        if (this.failedConnectionCallback) {
          await this.failedConnectionCallback(data.payload)
        }
        break
      case GQL_CONNECTION_KEEP_ALIVE:
        break
      /* istanbul ignore next */
      default:
        /* istanbul ignore next */
        console.error(`Can't process message: ${message}`)
        throw new Error(`Invalid message type: "${data.type}"`)
    }
  }

  startOperation (operation) {
    const { started, options, handler, extensions } = operation
    if (!started) {
      if (!this.ready) {
        throw new Error('Connection is not ready')
      }

      operation.started = true

      for (const eid of operation.eids)
        this.sendMessage(eid, GQL_START, options, extensions)
    }
  }

  oidFromEid (eid) {
    const [oid] = eid.split('_')

    return oid
  }

  createSubscription (query, variables, pubsub, connectionInit) {
    const operationId = hash({ query, variables, connectionInit })
    const eid = `${operationId}_${Date.now()}`

    if (this.operations.has(operationId)) {
      const operation = this.operations.get(operationId)

      this.operationsCount[operationId] = this.operationsCount[operationId] + 1

      operation.eids.add(eid)

      return operationId
    }

    const eids = new Set()

    eids.add(eid)

    const operation = {
      oid: operationId,
      eids,
      started: false,
      options: { context: pubsub.context, query, variables },
      handler: async (data) => {
        for (const eid of eids)
          await pubsub.publish({
            topic: `${this.serviceName}_${eid}`,
            payload: data
          })
      }
    }
    if (connectionInit) {
      operation.extensions = [
        {
          type: 'connectionInit',
          payload: connectionInit
        }
      ]
    }

    this.operations.set(operationId, operation)
    this.startOperation(operation)
    this.operationsCount[operationId] = 1

    pubsub.onclose = () => this.unsubscribe(operation, eid, false)

    pubsub.publish({
      topic: `${this.serviceName}_${eid}`,
      payload: operationId
    })

    return eid
  }
}

module.exports = SubscriptionClient
