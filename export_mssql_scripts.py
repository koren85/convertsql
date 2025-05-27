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
    print(f"User: {MSSQL_USER}")
    print(f"Database: {MSSQL_DB}")
    
    print("Установка соединения...")
    # Создаем соединение
    connection = DriverManager.getConnection(jdbc_url, MSSQL_USER, MSSQL_PASSWORD)
    
    print("✅ Соединение с MSSQL через JDBC установлено успешно!")
    
    # Проверяем количество строк до основного запроса
    check_statement = connection.createStatement()
    check_query = """
        SELECT COUNT(*) 
        FROM PPR_CALC_ALGORITHM
        WHERE a_id in
        (2075,
2105,
2141,
2216,
2240,
2332,
2508,
2528,
2543,
2557,
2662,
2663,
2695,
2696,
2698,
2712,
2760,
2830,
2832,
2833,
2834,
2865,
2867,
2876,
2878,
2909,
2928,
2945,
3002,
3004,
3026,
3085,
3161,
3209,
3210,
3211,
3212,
3213,
3214,
3215,
3255,
3272,
3274,
3275,
3276,
3277,
3284,
3285,
3305,
3329,
3345,
3350,
3354,
3355,
3356,
3357,
3358,
3359,
3360,
3361,
3362,
3376,
3474,
3477,
3478,
3480,
3481,
3482,
3483,
3484,
3485,
3493,
3503,
3507,
3518,
3521,
3522,
3523,
3545,
3561,
3562,
3563,
3566,
3568,
3570,
3590,
3604,
3611,
3667,
3704,
3713,
3714,
3721,
3737,
3753,
3761,
3780,
3792,
3838,
3867,
3868,
3869,
3870,
3871,
3896,
3912,
3913,
3920,
3921,
3933,
3941,
3942,
3944,
3951,
3964,
3965,
4008,
4020,
4021,
4022,
4032,
4048,
4049,
4050,
4051,
4057,
4058,
4059,
4060,
4061,
4062,
4064,
4065,
4091,
4092,
4104,
4108,
4113,
4131,
4134
)
    """
    check_result = check_statement.executeQuery(check_query)
    row_count = 0
    if check_result.next():
        row_count = check_result.getInt(1)
    print(f"⚠️ По вашему запросу найдено {row_count} записей в базе данных")
    check_result.close()
    check_statement.close()
    
    # Выполняем запрос
    statement = connection.createStatement()
    #resultSet = statement.executeQuery("""
    #    SELECT a_id, A_MSSQLFORMULA
    #    FROM PPR_CALC_ALGORITHM
    #    WHERE A_MSSQLFORMULA IS NOT NULL
    #    AND A_USESQL = 1
    #    AND A_MSSQLFORMULA LIKE '%[^}]%' -- Содержит что-то кроме закрывающих скобок
    #    AND (
    #        A_MSSQLFORMULA NOT LIKE '{%}%' -- Не начинается с {
     #       OR A_MSSQLFORMULA LIKE '% %' -- Содержит пробелы
      #      OR A_MSSQLFORMULA LIKE '%;%' -- Содержит точку с запятой
       #     OR A_MSSQLFORMULA LIKE '%SELECT%'
       #     OR A_MSSQLFORMULA LIKE '%FROM%'
       # )
   # """)
    
    resultSet = statement.executeQuery("""
        SELECT a_id, A_MSSQLFORMULA
        FROM PPR_CALC_ALGORITHM
        WHERE a_id in
        (2075,
2105,
2141,
2216,
2240,
2332,
2508,
2528,
2543,
2557,
2662,
2663,
2695,
2696,
2698,
2712,
2760,
2830,
2832,
2833,
2834,
2865,
2867,
2876,
2878,
2909,
2928,
2945,
3002,
3004,
3026,
3085,
3161,
3209,
3210,
3211,
3212,
3213,
3214,
3215,
3255,
3272,
3274,
3275,
3276,
3277,
3284,
3285,
3305,
3329,
3345,
3350,
3354,
3355,
3356,
3357,
3358,
3359,
3360,
3361,
3362,
3376,
3474,
3477,
3478,
3480,
3481,
3482,
3483,
3484,
3485,
3493,
3503,
3507,
3518,
3521,
3522,
3523,
3545,
3561,
3562,
3563,
3566,
3568,
3570,
3590,
3604,
3611,
3667,
3704,
3713,
3714,
3721,
3737,
3753,
3761,
3780,
3792,
3838,
3867,
3868,
3869,
3870,
3871,
3896,
3912,
3913,
3920,
3921,
3933,
3941,
3942,
3944,
3951,
3964,
3965,
4008,
4020,
4021,
4022,
4032,
4048,
4049,
4050,
4051,
4057,
4058,
4059,
4060,
4061,
4062,
4064,
4065,
4091,
4092,
4104,
4108,
4113,
4131,
4134
)
AND A_USESQL = 1
    """)


    # Сохраняем результаты
    count = 0
    total_rows = 0
    skipped_rows = 0
    
    while resultSet.next():
        total_rows += 1
        ouid = str(resultSet.getString(1))
        script_content = resultSet.getString(2)
        
        # Отладочная информация
        print(f"ID: {ouid}, Контент: {'НЕ ПУСТО' if script_content else 'ПУСТО'}")
        
        if script_content is None or script_content == "":
            skipped_rows += 1
            print(f"Пропуск ID {ouid}: пустая формула")
            continue
            
        # Преобразуем java.lang.String в Python str
        script_content = str(script_content)

        file_name = f'scripts/exported/{ouid}_exp_{count+1}.sql'
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        count += 1
        print(f"Сохранен скрипт {count}: {file_name}")
    
    print(f"Сохранено {count} скриптов в папку scripts/exported/")
    print(f"⚠️ Итог: всего найдено {row_count} записей, обработано {total_rows} записей, сохранено {count} файлов")
    if row_count != count:
        print(f"⚠️ ВНИМАНИЕ: Разница между количеством найденных и сохраненных записей: {row_count - count}")
        print("   Возможно, часть записей содержит NULL или пустые значения в поле A_MSSQLFORMULA")
    
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