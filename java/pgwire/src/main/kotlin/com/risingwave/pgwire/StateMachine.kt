package com.risingwave.pgwire

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.pgwire.database.*
import com.risingwave.pgwire.msg.CancelRequest
import com.risingwave.pgwire.msg.Messages
import com.risingwave.pgwire.msg.PgMessage
import com.risingwave.pgwire.msg.PgMsgType
import com.risingwave.pgwire.msg.Query
import com.risingwave.pgwire.msg.StartupMessage
import io.ktor.utils.io.ByteWriteChannel
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory

/**
 * StateMachine handles the message flow from a client connection. The entire message handling is
 * single-threaded.
 */
internal class StateMachine(private val out: ByteWriteChannel) {
  companion object {
    private val log = LoggerFactory.getLogger(StateMachine::class.java)
  }

  private var db: Database? = null

  var willTerminate: Boolean = false
    private set // read-only

  private var wBuf: ByteBuf = Unpooled.compositeBuffer()

  @Throws(PgException::class)
  suspend fun process(msg: PgMessage) {
    wBuf.clear()
    willTerminate = false
    when (msg.type) {
      PgMsgType.CancelRequest -> processCancelRequest(msg as CancelRequest)
      PgMsgType.Terminate -> processTerminate()
      PgMsgType.SSLRequest -> processSslRequest()
      PgMsgType.StartupMessage -> processStartupMessage(msg as StartupMessage)
      PgMsgType.Query -> processQuery(msg as Query)
      else -> throw IllegalStateException("Unexpected value: " + msg.type)
    }
    writePendingBuf()
  }

  suspend fun processPgError(err: PgException) {
    wBuf.clear()
    // In the event of an error, ErrorResponse is issued followed by ReadyForQuery.
    Messages.writeErrorResponse(err, wBuf)
    Messages.writeReadyForQuery(TransactionStatus.IDLE, wBuf)
    writePendingBuf()
  }

  // Write buffer to channel but may not fully flush.
  private suspend fun writePendingBuf() {
    if (wBuf.readableBytes() > 0) {
      out.writeFully(wBuf.nioBuffer())
      wBuf.clear()
    }
  }

  @Throws(PgException::class)
  private fun processStartupMessage(msg: StartupMessage) {
    Messages.writeAuthenticationOk(wBuf)
    // TODO(wutao): Implement the authentication phase.
    db = Databases.connect(msg.user, msg.database)
    Messages.writeParameterStatus("client_encoding", db!!.getServerEncoding(), wBuf)
    // In order to support simple protocol.
    Messages.writeParameterStatus("standard_conforming_strings", "on", wBuf)
    Messages.writeReadyForQuery(TransactionStatus.IDLE, wBuf)
  }

  private suspend fun processSslRequest() {
    // Unwilling to perform SSL.
    out.writeByte('N'.code.toByte())
  }

  @Throws(PgException::class)
  private suspend fun processQuery(msg: Query) {
    log.info("receive query: {}", msg.sql)
    if (db == null) {
      throw PgException(
              PgErrorCode.PROTOCOL_VIOLATION,
              "database has not been set up before query"
      )
    }
    val res = db!!.runStatement(msg.sql)
    if (res.isQuery) {
      processQueryWithResult(res)
    } else {
      Messages.writeCommandComplete(res.statementType, res.effectedRowsCnt, wBuf)
    }
    Messages.writeReadyForQuery(TransactionStatus.IDLE, wBuf)
  }

  @Throws(PgException::class)
  private suspend fun processQueryWithResult(res: PgResult) {
    val it = res.createIterator()
    Messages.writeRowDescription(it.rowDesc, wBuf)
    writePendingBuf()

    var rowsCnt = 0
    while (it.next()) {
      Messages.writeDataRow(it.row, wBuf)
      // Flush buffer for every DataRow to prevent OOM caused by very large result.
      writePendingBuf()
      rowsCnt++
    }
    Messages.writeCommandComplete(res.statementType, rowsCnt, wBuf)
  }

  private fun processCancelRequest(cancel: CancelRequest) {
    log.info("Received cancellation {} {}", cancel.processId, cancel.secret)
    this.willTerminate = true
  }

  private fun processTerminate() {
    this.willTerminate = true
  }
}
