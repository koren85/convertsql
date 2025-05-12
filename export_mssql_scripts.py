import os
import jpype
import jpype.imports
from dotenv import load_dotenv
import sys

load_dotenv()

# Настройка JAVA_HOME для Python
java_home = '/opt/homebrew/opt/openjdk'  # Путь на M1/M2 Mac
if not os.path.exists(java_home):
    java_home = '/usr/local/opt/openjdk'  # Путь на Intel Mac

os.environ['JAVA_HOME'] = java_home
print(f"Using JAVA_HOME: {java_home}")

MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DB = os.getenv('MSSQL_DB')

# Создаем директорию для скриптов заранее
os.makedirs('scripts/exported', exist_ok=True)

# Путь к JDBC драйверу (УБЕДИСЬ, ЧТО ТЫ ЕГО СКАЧАЛ!)
jdbc_driver_path = './lib/mssql-jdbc.jar'  # путь к скачанному JDBC драйверу

# Проверка наличия драйвера
if not os.path.exists(jdbc_driver_path):
    print(f"❌ Ошибка: JDBC драйвер не найден по пути {jdbc_driver_path}")
    print("Скачай драйвер с https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server")
    sys.exit(1)

try:
    print("Запуск JVM...")
    # Запускаем JVM с явным указанием пути к jvm.dll/libjvm.so
    if not jpype.isJVMStarted():
        jvm_path = jpype.getDefaultJVMPath()
        print(f"JVM path: {jvm_path}")
        jpype.startJVM(jvm_path, f"-Djava.class.path={jdbc_driver_path}")
    
    print("JVM запущена успешно")
    
    # Импортируем Java классы
    from java.sql import DriverManager, SQLException
    
    # JDBC строка подключения (идентична твоей из DataGrip)
    jdbc_url = f'jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName={MSSQL_DB};trustServerCertificate=true;encrypt=false'
    print(f"JDBC URL: {jdbc_url}")
    
    print("Установка соединения...")
    # Создаем соединение
    connection = DriverManager.getConnection(jdbc_url, MSSQL_USER, MSSQL_PASSWORD)
    
    print("✅ Соединение с MSSQL через JDBC установлено успешно!")
    
    # Выполняем запрос
    statement = connection.createStatement()
    resultSet = statement.executeQuery("SELECT a_id, A_MSSQLFORMULA FROM PPR_CALC_ALGORITHM")
    
    # Сохраняем результаты
    count = 0
    
    while resultSet.next():
        ouid = str(resultSet.getString(1))
        script_content = resultSet.getString(2) or ""
        if script_content is None:
            script_content = ""
        else:
            # Преобразуем java.lang.String в Python str
            script_content = str(script_content)

        file_name = f'scripts/exported/{ouid}_exp_{count+1}.sql'
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        count += 1
        print(f"Сохранен скрипт {count}: {file_name}")
    
    print(f"Сохранено {count} скриптов в папку scripts/exported/")
    
    # Закрываем ресурсы
    resultSet.close()
    statement.close()
    connection.close()
    
except Exception as e:
    print("❌ Ошибка при подключении через JDBC:")
    print(e)
    import traceback
    traceback.print_exc()
finally:
    # Останавливаем JVM
    if jpype.isJVMStarted():
        jpype.shutdownJVM()