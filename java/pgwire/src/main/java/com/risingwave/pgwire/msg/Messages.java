package com.risingwave.pgwire.msg;

import com.risingwave.pgwire.database.PgErrorCode;
import com.risingwave.pgwire.database.PgException;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.TransactionStatus;
import com.risingwave.pgwire.types.PgValue;
import io.netty.buffer.ByteBuf;
import java.util.List;

public class Messages {

  public static PgMessage createRegularPacket(byte tag, byte[] buf) throws PgException {
    PgMessage ret;
    switch (tag) {
      case 'Q':
        ret = new Query();
        break;
      case 'X':
        ret = new PgMessage(PgMsgType.Terminate);
        break;
      default:
        throw new PgException(
            PgErrorCode.FEATURE_NOT_SUPPORTED, String.format("unsupported message tag: %s", tag));
    }
    ret.decodeFrom(buf);
    return ret;
  }

  public static PgMessage createStartupPacket(int protocol, byte[] buf) throws PgException {
    PgMessage ret;
    switch (protocol) {
      case 196608:
        // The protocol version number.
        // The most significant 16 bits are the major version number (3).
        // The least significant 16 bits are the minor version number (0).
        ret = new StartupMessage();
        break;
      case 80877103:
        // The SSL request code.
        ret = new PgMessage(PgMsgType.SSLRequest);
        break;
      case 80877102:
        // The cancel request code.
        ret = new CancelRequest();
        break;
      default:
        throw new PgException(
            PgErrorCode.PROTOCOL_VIOLATION, "unexpected protocol number %d", protocol);
    }
    ret.decodeFrom(buf);
    return ret;
  }

  // AuthenticationOk
  // +-----+----------+-----------+
  // | 'R' | int32(8) | int32(0)  |
  // +-----+----------+-----------+
  public static void writeAuthenticationOk(ByteBuf buf) {
    buf.writeByte('R');
    buf.writeInt(8);
    buf.writeInt(0);
  }

  // CommandComplete
  // +-----+-----------+-----------------+
  // | 'C' | int32 len | str commandTag  |
  // +-----+-----------+-----------------+
  public static void writeCommandComplete(String stmtType, int effectedRowsCnt, ByteBuf buf) {
    CommandComplete complete = new CommandComplete(stmtType, effectedRowsCnt);
    buf.writeByte('C');
    buf.writeInt(4 + complete.getTag().length() + 1);
    buf.writeBytes(complete.getTag().getBytes());
    buf.writeByte('\0');
  }

  // DataRow
  // +-----+-----------+--------------+--------+-----+--------+
  // | 'D' | int32 len | int16 colNum | column | ... | column |
  // +-----+-----------+--------------+----+---+-----+--------+
  //                                       |
  //                          +-----------+v------+
  //                          | int32 len | bytes |
  //                          +-----------+-------+
  public static void writeDataRow(List<PgValue> columns, ByteBuf buf) {
    buf.writeByte('D');
    buf.writeInt(0); // Placeholder.
    buf.writeShort((short) columns.size());

    int totalLen = 4 + 2 + columns.size() * 4;
    for (PgValue val : columns) {
      if (val == null) {
        // -1 indicates a NULL value.
        buf.writeInt(-1);
        continue;
      }
      String valData = val.encodeInText();
      buf.writeInt(valData.length());
      buf.writeBytes(valData.getBytes());
      totalLen += valData.length();
    }
    buf.setInt(1, totalLen);
  }

  // ParameterStatus
  // +-----+-----------+----------+------+-----------+------+
  // | 'S' | int32 len | str name | '\0' | str value | '\0' |
  // +-----+-----------+----------+------+-----------+------+
  //
  // At present there is a hard-wired set of parameters for which
  // ParameterStatus will be generated: they are:
  //  server_version,
  //  server_encoding,
  //  client_encoding,
  //  application_name,
  //  is_superuser,
  //  session_authorization,
  //  DateStyle,
  //  IntervalStyle,
  //  TimeZone,
  //  integer_datetimes,
  //  standard_conforming_string
  //
  // See: https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC.
  //
  public static void writeParameterStatus(String name, String value, ByteBuf buf) {
    buf.writeByte('S');
    buf.writeInt(4 + name.length() + 1 + value.length() + 1);
    buf.writeBytes(name.getBytes());
    buf.writeByte('\0');
    buf.writeBytes(value.getBytes());
    buf.writeByte('\0');
  }

  // ReadyForQuery
  // +-----+----------+---------------------------+
  // | 'Z' | int32(5) | byte1(transaction status) |
  // +-----+----------+---------------------------+
  public static void writeReadyForQuery(TransactionStatus txnStatus, ByteBuf buf) {
    buf.writeByte('Z');
    buf.writeInt(5);
    buf.writeByte(txnStatus.asChar());
  }

  // RowDescription
  // +-----+-----------+--------------+-------+-----+-------+
  // | 'T' | int32 len | int16 colNum | field | ... | field |
  // +-----+-----------+--------------+----+--+-----+-------+
  //                                       |
  // +---------------+-------+-------+-----v-+-------+-------+-------+
  // | str fieldName | int32 | int16 | int32 | int16 | int32 | int16 |
  // +---------------+---+---+---+---+---+---+----+--+---+---+---+---+
  //                     |       |       |        |      |       |
  //                     v       |       v        v      |       v
  //                tableOID     |    typeOID  typeLen   |   formatCode
  //                             v                       v
  //                        colAttrNum               typeModifier
  public static void writeRowDescription(List<PgFieldDescriptor> fds, ByteBuf buf) {
    buf.writeByte('T');
    int len = 4 + 2 + fds.size() * (4 + 2 + 4 + 2 + 4 + 2);
    for (PgFieldDescriptor fd : fds) {
      len += fd.name.length() + 1;
    }
    buf.writeInt(len);
    buf.writeShort((short) fds.size());
    for (PgFieldDescriptor fd : fds) {
      buf.writeBytes(fd.name.getBytes());
      buf.writeByte('\0');

      buf.writeInt(fd.tableOid);
      buf.writeShort(fd.colAttrNum);
      buf.writeInt(fd.typeOid);
      buf.writeShort(fd.typeLen);
      buf.writeInt(fd.typeModifier);
      buf.writeShort(fd.formatCode);
    }
  }

  // ErrorResponse
  // +-----+-----------+-------+-----+-------+
  // | 'E' | int32 len | field | ... | field |
  // +-----+-----------+-------+-----+-------+
  //                       |
  //               +-------v---------+-------------------+
  //               | byte1 fieldType | string fieldValue |
  //               +-----------------+-------------------+
  public static void writeErrorResponse(PgException err, ByteBuf buf) {
    int totalLen = 4; // header
    totalLen += 1 + 1; // 'S' severity '\0'
    totalLen += 1 + err.getMessage().length() + 1; // 'M' message '\0'
    totalLen += 1 + 5 + 1; // 'C' error_code '\0'
    totalLen += 1;

    buf.writeByte('E');
    buf.writeInt(totalLen);
    buf.writeByte('S');
    buf.writeByte('\0');
    buf.writeByte('M');
    buf.writeBytes(err.getMessage().getBytes());
    buf.writeByte('\0');
    buf.writeByte('C');
    buf.writeBytes(err.getCode().code().getBytes());
    buf.writeByte('\0');
    buf.writeByte('\0');
  }
}
