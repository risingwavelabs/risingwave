package com.risingwave.pgwire

import com.risingwave.pgwire.database.PgException
import com.risingwave.pgwire.database.PgErrorCode
import com.risingwave.pgwire.msg.Messages
import com.risingwave.pgwire.msg.PgMessage
import com.risingwave.pgwire.msg.PgMsgType
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import java.io.IOException
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory

class PgProtocol(private val input: ByteReadChannel, private val output: ByteWriteChannel) {
  companion object {
    private val log = LoggerFactory.getLogger(PgServerConn::class.java)
  }

  private var startedUp: Boolean = false
  private var stm: StateMachine = StateMachine(output)

  /** Process one client message and reply with response if any. */
  suspend fun process(): Boolean {
    try {
      doProcess()
      return stm.willTerminate
    } catch (err: PgException) {
      stm.processPgError(err)
      return false
    }
  }

  @Throws(PgException::class)
  private suspend fun doProcess() {
    try {
      val msg = readMessage()
      stm.process(msg)
      if (msg.type == PgMsgType.StartupMessage) {
        startedUp = true
      }
    } catch (exp: IOException) {
      throw PgException(PgErrorCode.CONNECTION_EXCEPTION, exp)
    } catch (exp: PgException) {
      throw exp
    } catch (exp: Throwable) {
      throw PgException(PgErrorCode.INTERNAL_ERROR, exp)
    }
  }

  private suspend fun readMessage(): PgMessage {
    if (!startedUp) {
      return readStartupPacket()
    }
    return readRegularPacket()
  }

  // Regular Packet
  // +----------+-----------+---------+
  // | char tag | int32 len | payload |
  // +----------+-----------+---------+
  private suspend fun readRegularPacket(): PgMessage {
    val tag = input.readByte()
    val len = input.readInt()

    val payload = ByteBuffer.allocate(len - 4)
    if (len - 4 > 0) {
      input.readFully(payload)
    }
    return Messages.createRegularPacket(tag, payload.array())
  }

  // Startup Packet
  // +-----------+----------------+---------+
  // | int32 len | int32 protocol | payload |
  // +-----------+----------------+---------+
  private suspend fun readStartupPacket(): PgMessage {
    val len = input.readInt()
    val protocol = input.readInt()

    val payload = ByteBuffer.allocate(len - 8)
    if (len - 8 > 0) {
      input.readFully(payload)
    }
    return Messages.createStartupPacket(protocol, payload.array())
  }
}
