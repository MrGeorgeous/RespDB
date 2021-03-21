# Лаб №1. Логика сохранения и получения значения по ключу

#### Георгий Семенов M32011
[Последний тест](http://77.234.215.138:28090/job/Semenov-lab1/57/console)

Устройство файловой системы: `workspaceRoot/dbName/tableName/segmentName_timestamp`

Интерфейс реализации:
```java
public class DatabaseImpl implements Database {

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException;
    public String getName();
    
    public void createTableIfNotExists(String tableName) throws DatabaseException;
    
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException;
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException;
    public void delete(String tableName, String objectKey) throws DatabaseException;
    
}
```
