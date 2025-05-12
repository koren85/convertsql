# convertsql

## Подключение к MS SQL через JDBC на macOS

### 1. Установи Java (JDK)

Рекомендуется OpenJDK 17+ (подходит и 23):

```bash
brew install openjdk@17
```

После установки добавь в ~/.zshrc или ~/.bash_profile:

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
export PATH="$JAVA_HOME/bin:$PATH"
```

Перезапусти терминал:
```bash
source ~/.zshrc  # или source ~/.bash_profile
```

Проверь:
```bash
java -version
```

### 2. Скачай JDBC драйвер Microsoft для SQL Server

- Перейди на https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
- Скачай архив, распакуй, возьми файл вида `mssql-jdbc-XX.X.XXX.jre8.jar` (или jre11/jre17 — любой, кроме jreX-preview)
- Положи .jar в корень проекта или в папку `lib/`
- Для простоты можешь переименовать в `mssql-jdbc.jar`

### 3. Установи Python-зависимости

```bash
pip install jpype1 python-dotenv
```

### 4. Пример кода для подключения через JDBC (JPype)

```python
import os
import jpype
import jpype.imports
from dotenv import load_dotenv

load_dotenv()

# Укажи путь к JAVA_HOME (без /bin/java!)
java_home = '/opt/homebrew/opt/openjdk'  # M1/M2 Mac
if not os.path.exists(java_home):
    java_home = '/usr/local/opt/openjdk'  # Intel Mac
os.environ['JAVA_HOME'] = java_home

# Путь к JDBC драйверу
jdbc_driver_path = './mssql-jdbc.jar'

# Запуск JVM
if not jpype.isJVMStarted():
    jvm_path = jpype.getDefaultJVMPath()
    jpype.startJVM(jvm_path, f"-Djava.class.path={jdbc_driver_path}")

from java.sql import DriverManager

MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DB = os.getenv('MSSQL_DB')

jdbc_url = f'jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName={MSSQL_DB};trustServerCertificate=true;encrypt=false'

connection = DriverManager.getConnection(jdbc_url, MSSQL_USER, MSSQL_PASSWORD)
# ... дальше работаешь с connection ...
```

### 5. Важно!
- Не указывай в JAVA_HOME путь до bin/java, только до корня JDK!
- Если JVM не стартует — проверь, что путь к libjvm.dylib корректный (обычно внутри JAVA_HOME)
- Если ошибка "Invalid column name" — проверь имена столбцов в своей таблице
- Для работы с Java-строками всегда делай `str(java_string)` перед записью в файл

---
