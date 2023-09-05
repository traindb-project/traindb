/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.engine;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.engine.nio.ByteBuffers;
import traindb.engine.nio.Message;
import traindb.engine.nio.MessageStream;
import traindb.jdbc.Driver;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.jdbc.TrainDBJdbc41Factory;
import traindb.jdbc.TrainDBStatement;
import traindb.schema.SchemaManager;
import traindb.sql.TrainDBSql;
import traindb.sql.TrainDBSqlCommand;
import traindb.sql.calcite.TrainDBSqlCalciteParserImpl;

public final class Session implements Runnable {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(Session.class);
  private static final ThreadLocal<Session> LOCAL_SESSION = new ThreadLocal<>();
  private final CancelContext cancelContext;
  private final int sessionId;

  private final SocketChannel clientChannel;
  private final EventHandler eventHandler;
  private final SchemaManager schemaManager;
  final MessageStream messageStream;

  Session(SocketChannel clientChannel, EventHandler eventHandler, SchemaManager schemaManager) {
    sessionId = new Random(this.hashCode()).nextInt();
    cancelContext = new CancelContext(this);
    this.clientChannel = clientChannel;
    this.eventHandler = eventHandler;
    this.schemaManager = schemaManager;
    this.messageStream = new MessageStream(clientChannel);
  }

  public static Session currentSession() {
    Session currSess = LOCAL_SESSION.get();
    if (currSess == null) {
      throw new RuntimeException("current session does not exist");
    }

    return LOCAL_SESSION.get();
  }

  public int getId() {
    return sessionId;
  }

  public boolean isCanceled() {
    return cancelContext.isCanceled();
  }

  @Override
  public void run() {
    LOCAL_SESSION.set(this);
    try {
      messageLoop();
    } catch (Exception e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
    }
    LOCAL_SESSION.remove();

    close();
  }

  public void sendError(Exception e) throws IOException {
    Message.Builder builder = Message.builder('E')
            .putChar('S').putCString("ERROR")
            .putChar('C').putCString("")
            .putChar('M').putCString(e.getMessage())
            .putChar('\0');
    messageStream.putMessageAndFlush(builder.build());
  }

  private void messageLoop() throws Exception {
    SessionHandler sessHandler = new SessionHandler();

    while (true) {
      try {
        Message msg = messageStream.getMessage();
        char type = msg.getType();
        LOG.debug("received data type=" + msg.getType());

        // TODO: handle messages
        switch (type) {
          case 'S':
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonMsg = (JSONObject) jsonParser.parse(msg.getBodyString());
            Properties info = new Properties();
            info.put("user", jsonMsg.get("user").toString());
            info.put("password", jsonMsg.get("password").toString());
            sessHandler.setConnection(makeConnection(jsonMsg.get("url").toString(), info));
            break;
          case 'E':
            sessHandler.handleQuery(msg.getBodyString());
            break;
          default:
            throw new TrainDBException("invalid message type '" + type + "'");
        }
      } catch (Exception e) {
        throw new TrainDBException(e.getMessage());
      }
    }
  }

  private TrainDBConnectionImpl makeConnection(String url, Properties info) throws SQLException {
    try {
      String newUrl = url;
      int urlPrefixLength = 0;
      String[] tokens = url.split(":");
      if (tokens.length >= 2 && (tokens[1].equalsIgnoreCase("traindb"))) {
        List<String> newTokens = new ArrayList<>();
        for (int i = 0; i < tokens.length; ++i) {
          if (i != 1) {
            newTokens.add(tokens[i]);
          }
        }
        if (tokens.length >= 3 && tokens[2].startsWith("//")) {
          newUrl = null; // no datasource connection
        } else {
          newUrl = Joiner.on(":").join(newTokens);
        }
        String[] suffixTokens = url.split("\\?");
        urlPrefixLength = suffixTokens[0].length();
      }
      String urlSuffix = url.substring(urlPrefixLength);
      Properties info2 = ConnectStringParser.parse(urlSuffix, info);

      TrainDBJdbc41Factory factory = new TrainDBJdbc41Factory();
      return factory.newConnection(new Driver(), factory, newUrl, info2, null, null, schemaManager);
    } catch (Exception e) {
      throw new SQLException(e.getMessage());
    }
  }


  class SessionHandler {
    private TrainDBConnectionImpl conn;
    private TrainDBStatement stmt;
    private SqlParser.Config parserConfig;

    SessionHandler() {
      this.conn = null;
      this.stmt = null;
    }

    private void checkConnection() throws TrainDBException {
      if (conn == null) {
        throw new TrainDBException("no datasource connection");
      }
    }

    public void setConnection(TrainDBConnectionImpl newConn) {
      try {
        if (conn != null) {
          if (stmt != null) {
            stmt.close();
          }
          conn.close();
        }
      } catch (SQLException e) {
        LOG.debug("old connection close error");
      }
      if (newConn == null) {
        return;
      }
      conn = newConn;

      final CalciteConnectionConfig config = conn.config();
      parserConfig = SqlParser.config()
          .withQuotedCasing(config.quotedCasing())
          .withUnquotedCasing(config.unquotedCasing())
          .withQuoting(config.quoting())
          .withConformance(config.conformance())
          .withCaseSensitive(config.caseSensitive());
      final SqlParserImplFactory parserFactory =
          config.parserFactory(SqlParserImplFactory.class, TrainDBSqlCalciteParserImpl.FACTORY);
      if (parserFactory != null) {
        parserConfig = parserConfig.withParserFactory(parserFactory);
      }
    }

    public void handleQuery(String sqlQuery) throws TrainDBException, IOException {
      checkConnection();
      LOG.debug("handleQuery: " + sqlQuery);

      try {
        if (stmt == null) {
          stmt = conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, conn.getHoldability());
        }

        // First check input query with TrainDB sql grammar
        List<TrainDBSqlCommand> commands = null;
        try {
          commands = TrainDBSql.parse(sqlQuery, parserConfig);
        } catch (Exception e) {
          if (commands != null) {
            commands.clear();
          }
        }

        if (commands != null && commands.size() > 0
            && !isTrainDBStmtWithResultSet(commands.get(0).getType())) {  // TrainDB DDL
          stmt.execute(sqlQuery);
          sendCommandComplete(commands.get(0).getType().toString());
        } else {
          // stmt.setFetchSize(0);
          ResultSet rs = stmt.executeQuery(sqlQuery);
          sendRowDesc(rs.getMetaData());
          sendDataRow(rs);
          sendCommandComplete("SELECT"); // FIXME
        }
      } catch (IOException ioe) {
        sendError(ioe);
      } catch (SQLException se) {
        sendError(se);
      }
    }

  }

  private boolean isTrainDBStmtWithResultSet(TrainDBSqlCommand.Type type) {
    return type.toString().startsWith("SHOW")
        || type.toString().startsWith("DESCRIBE")
        || type.toString().startsWith("EXPORT");
  }

  private void sendRowDesc(ResultSetMetaData md) {
    try {
      int columnCount = md.getColumnCount();
      Message.Builder msgBld = Message.builder('T').putShort((short) columnCount);
      for (int i = 1; i <= columnCount; i++) {
        msgBld.putCString(md.getColumnName(i));
        int type = md.getColumnType(i);
        msgBld.putInt(type);
        if (type == Types.VARCHAR) {
          msgBld.putInt(md.getPrecision(i));
          msgBld.putShort((short) 0); // Field.TEXT_FORMAT
        } else {
          msgBld.putInt(getTypeSize(type));
          msgBld.putShort((short) 1); // Field.BINARY_FORMAT
        }
      }
      messageStream.putMessage(msgBld.build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void sendDataRow(ResultSet rs) throws IOException {
    try {
      ResultSetMetaData md = rs.getMetaData();
      int columnCount = md.getColumnCount();
      boolean noData = true;
      while (rs.next()) {
        Message.Builder msgBld = Message.builder('D').putShort((short) columnCount);
        // Column Data
        for (int i = 1; i <= columnCount; i++) {
          int type = md.getColumnType(i);
          switch (type) {
            case Types.TINYINT:
              msgBld.putInt(getTypeSize(type)).putByte(rs.getByte(i));
              break;
            case Types.SMALLINT:
              msgBld.putInt(getTypeSize(type)).putShort(rs.getShort(i));
              break;
            case Types.INTEGER:
              msgBld.putInt(getTypeSize(type)).putInt(rs.getInt(i));
              break;
            case Types.BIGINT:
              msgBld.putInt(getTypeSize(type)).putLong(rs.getLong(i));
              break;
            case Types.FLOAT:
              msgBld.putInt(getTypeSize(type)).putFloat(rs.getFloat(i));
              break;
            case Types.DOUBLE:
              msgBld.putInt(getTypeSize(type)).putDouble(rs.getDouble(i));
              break;
            case Types.VARCHAR: {
              byte[] bytes = rs.getString(i).getBytes(StandardCharsets.UTF_8);
              msgBld.putInt(bytes.length).putBytes(bytes);
              break;
            }
            case Types.TIMESTAMP: {
              Timestamp ts = rs.getTimestamp(i);
              byte[] bytes = ts.toString().getBytes(StandardCharsets.UTF_8);
              msgBld.putInt(bytes.length).putBytes(bytes);
              break;
            }
            case Types.VARBINARY: {
              byte[] bytes = rs.getBytes(i);
              msgBld.putInt(bytes.length).putBytes(bytes);
              break;
            }
            // TODO support more data types
            default:
              throw new TrainDBException("Not supported data type: " + type);
          }
          noData = false;
        }
        messageStream.putMessage(msgBld.build());
      }
      if (noData) {
        sendNoData();
      }
    } catch (SQLException e) {
      sendError(e);
    } catch (TrainDBException e) {
      sendError(e);
    }
  }

  private void sendNoData() throws IOException {
    LOG.debug("send NoData message");
    messageStream.putMessage(Message.builder('n').build());
  }

  private int getTypeSize(int type) {
    switch (type) {
      case Types.TINYINT:
        return ByteBuffers.BYTE_BYTES;
      case Types.SMALLINT:
        return ByteBuffers.SHORT_BYTES;
      case Types.INTEGER:
        return ByteBuffers.INTEGER_BYTES;
      case Types.BIGINT:
      case Types.TIMESTAMP:
        return ByteBuffers.LONG_BYTES;
      case Types.FLOAT:
        return ByteBuffers.FLOAT_BYTES;
      case Types.DOUBLE:
        return ByteBuffers.DOUBLE_BYTES;
      default:
        return -1;
    }
  }

  private void sendCommandComplete(String tag) throws IOException {
    LOG.debug("send CommandComplete message");
    if (tag == null) {
      tag = "";
    }
    Message msg = Message.builder('C')
        .putCString(tag)
        .build();
    messageStream.putMessageAndFlush(msg);
  }

  void reject() {
    close();
  }

  void close() {
    try {
      clientChannel.close();
    } catch (IOException e) {
      LOG.info(ExceptionUtils.getStackTrace(e));
    }

    eventHandler.onClose(this);
  }

  void cancel() {
    cancelContext.cancel();
  }

  interface EventHandler {
    void onClose(Session session);

    void onCancel(int sessionId);
  }
}
