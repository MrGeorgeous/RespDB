# RespDB

This project was conducted during the 'Programming technologies' (2021) university course

The task was to implement given interfaces and methods for Client-Server database application using sockets and Resp-protocol.

### Description

Start `JavaSocketServerConnector.java` to launch the server.

```
+---basics
|   |   DatabaseServer.java
|   |
|   +---config
|   |       ConfigLoader.java
|   |       DatabaseConfig.java
|   |       DatabaseServerConfig.java
|   |       ServerConfig.java
|   |
|   +---connector
|   |       JavaSocketServerConnector.java
|   |
|   +---console
|   |   |   DatabaseApiSerializable.java
|   |   |   DatabaseCommand.java
|   |   |   DatabaseCommandArgPositions.java
|   |   |   DatabaseCommandResult.java
|   |   |   DatabaseCommands.java
|   |   |   ExecutionEnvironment.java
|   |   |
|   |   \---impl
|   |           CreateDatabaseCommand.java
|   |           CreateTableCommand.java
|   |           DeleteKeyCommand.java
|   |           ExecutionEnvironmentImpl.java
|   |           FailedDatabaseCommandResult.java
|   |           GetKeyCommand.java
|   |           SetKeyCommand.java
|   |           SuccessDatabaseCommandResult.java
|   |
|   +---exceptions
|   |       DatabaseException.java
|   |
|   +---index
|   |   |   KvsIndex.java
|   |   |   SegmentOffsetInfo.java
|   |   |
|   |   \---impl
|   |           MapBasedKvsIndex.java
|   |           SegmentIndex.java
|   |           SegmentOffsetInfoImpl.java
|   |           TableIndex.java
|   |
|   +---initialization
|   |   |   DatabaseInitializationContext.java
|   |   |   InitializationContext.java
|   |   |   Initializer.java
|   |   |   SegmentInitializationContext.java
|   |   |   TableInitializationContext.java
|   |   |
|   |   \---impl
|   |           DatabaseInitializationContextImpl.java
|   |           DatabaseInitializer.java
|   |           DatabaseServerInitializer.java
|   |           InitializationContextImpl.java
|   |           SegmentInitializationContextImpl.java
|   |           SegmentInitializer.java
|   |           TableInitializationContextImpl.java
|   |           TableInitializer.java
|   |
|   +---logic
|   |   |   Database.java
|   |   |   DatabaseCache.java
|   |   |   DatabaseFactory.java
|   |   |   DatabaseRecord.java
|   |   |   Segment.java
|   |   |   Table.java
|   |   |   WritableDatabaseRecord.java
|   |   |
|   |   +---impl
|   |   |       CachingTable.java
|   |   |       DatabaseCacheImpl.java
|   |   |       DatabaseImpl.java
|   |   |       RemoveDatabaseRecord.java
|   |   |       SegmentImpl.java
|   |   |       SetDatabaseRecord.java
|   |   |       TableImpl.java
|   |   |
|   |   \---io
|   |           DatabaseInputStream.java
|   |           DatabaseOutputStream.java
|   |
|   \---resp
|           CommandReader.java
|
+---client
|   +---client
|   |       KvsClient.java
|   |       SimpleKvsClient.java
|   |
|   +---command
|   |       CreateDatabaseKvsCommand.java
|   |       CreateTableKvsCommand.java
|   |       DeleteKvsCommand.java
|   |       GetKvsCommand.java
|   |       KvsCommand.java
|   |       SetKvsCommand.java
|   |
|   +---connection
|   |       ConnectionConfig.java
|   |       DirectReferenceKvsConnection.java
|   |       KvsConnection.java
|   |       SocketKvsConnection.java
|   |
|   \---exception
|           ConnectionException.java
|           DatabaseExecutionException.java
|
\---protocol
    |   RespReader.java
    |   RespWriter.java
    |
    \---model
            RespArray.java
            RespBulkString.java
            RespCommandId.java
            RespError.java
            RespObject.java

```
