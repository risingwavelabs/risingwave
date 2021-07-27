package com.risingwave.pgwire

import com.risingwave.pgwire.database.DatabaseManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import org.slf4j.LoggerFactory

/** A tcp server-side connection. */
class PgServerConn(private val socket: Socket) {
  companion object {
    private val log = LoggerFactory.getLogger(PgServerConn::class.java)
  }

  suspend fun serve() {
    val remoteAddress = "${socket.remoteAddress}"
    log.info("Accepted new connection: $remoteAddress")
    try {
      val input: ByteReadChannel = socket.openReadChannel()
      val output: ByteWriteChannel = socket.openWriteChannel(autoFlush = true)
      val proto = PgProtocol(input, output)
      while (true) {
        val terminate = proto.process()
        if (terminate) {
          break
        }
      }
    } catch (e: Throwable) {
      log.error(e.message)
      // Possible blocking point. `close` is not a suspendable method.
      socket.close()
    }
  }
}
