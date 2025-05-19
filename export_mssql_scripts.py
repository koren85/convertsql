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
        (2125,
2365,
2483,
2567,
2589,
2848,
2935,
3003,
3138,
3278,
3295,
3296,
3300,
3304,
3353,
3434,
3435,
3436,
3437,
3438,
3439,
3440,
3442,
3443,
3444,
3451,
3455,
3456,
3457,
3458,
3467,
3468,
3479,
3490,
3496,
3497,
3506,
3510,
3524,
3525,
3526,
3527,
3528,
3529,
3544,
3580,
3582,
3583,
3584,
3585,
3653,
3654,
3655,
3656,
3657,
3658,
3659,
3660,
3672,
3674,
3677,
3682,
3683,
3685,
3699,
3706,
3707,
3719,
3720,
3724,
3725,
3730,
3731,
3739,
3744,
3754,
3759,
3760,
3763,
3772,
3776,
3819,
3821,
3826,
3836,
3839,
3841,
3842,
3843,
3844,
3845,
3846,
3847,
3848,
3849,
3850,
3851,
3852,
3853,
3854,
3855,
3856,
3857,
3858,
3859,
3860,
3862,
3863,
3864,
3865,
3874,
3875,
3876,
3877,
3897,
3899,
3922,
3936,
3937,
3938,
3939,
3940,
3945,
3946,
3947,
3948,
3949,
3950,
3987,
3988,
3992,
3993,
3995,
3996,
4019,
4023,
4024,
4025,
4026,
4033,
4038,
4039,
4040,
4041,
4042,
4044,
4045,
4046,
4047,
4056,
4063,
4066,
4082,
4083,
4093,
4094,
4095,
4101,
4102,
4103,
4105,
4118,
4120,
4121,
4122,
4123,
4128,
4129,
4130,
4133,
4135,
4136,
4137,
4138,
4139,
4140,
4141)
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
        (2125,
2365,
2483,
2567,
2589,
2848,
2935,
3003,
3138,
3278,
3295,
3296,
3300,
3304,
3353,
3434,
3435,
3436,
3437,
3438,
3439,
3440,
3442,
3443,
3444,
3451,
3455,
3456,
3457,
3458,
3467,
3468,
3479,
3490,
3496,
3497,
3506,
3510,
3524,
3525,
3526,
3527,
3528,
3529,
3544,
3580,
3582,
3583,
3584,
3585,
3653,
3654,
3655,
3656,
3657,
3658,
3659,
3660,
3672,
3674,
3677,
3682,
3683,
3685,
3699,
3706,
3707,
3719,
3720,
3724,
3725,
3730,
3731,
3739,
3744,
3754,
3759,
3760,
3763,
3772,
3776,
3819,
3821,
3826,
3836,
3839,
3841,
3842,
3843,
3844,
3845,
3846,
3847,
3848,
3849,
3850,
3851,
3852,
3853,
3854,
3855,
3856,
3857,
3858,
3859,
3860,
3862,
3863,
3864,
3865,
3874,
3875,
3876,
3877,
3897,
3899,
3922,
3936,
3937,
3938,
3939,
3940,
3945,
3946,
3947,
3948,
3949,
3950,
3987,
3988,
3992,
3993,
3995,
3996,
4019,
4023,
4024,
4025,
4026,
4033,
4038,
4039,
4040,
4041,
4042,
4044,
4045,
4046,
4047,
4056,
4063,
4066,
4082,
4083,
4093,
4094,
4095,
4101,
4102,
4103,
4105,
4118,
4120,
4121,
4122,
4123,
4128,
4129,
4130,
4133,
4135,
4136,
4137,
4138,
4139,
4140,
4141)

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