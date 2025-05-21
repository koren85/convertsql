import os
import sys
import glob
import re
from dotenv import load_dotenv

load_dotenv()

# Определение типа СУБД из env
DB_PROVIDER = os.getenv('DB_PROVIDER', '').lower()  # postgres или mssql

# Проверка типа СУБД
if DB_PROVIDER not in ['postgres', 'mssql']:
    print(f"❌ Ошибка: Неизвестный тип СУБД: {DB_PROVIDER}")
    print("Укажите DB_PROVIDER=postgres или DB_PROVIDER=mssql в файле .env")
    sys.exit(1)

# Загрузка параметров подключения
DB_HOST = os.getenv('DST_HOST')
DB_PORT = os.getenv('DST_PORT')
DB_USER = os.getenv('DST_USER')
DB_PASSWORD = os.getenv('DST_PASSWORD')
DB_NAME = os.getenv('DST_DATABASE')

# Проверка параметров подключения
if not all([DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME]):
    print(f"❌ Ошибка: Не все параметры подключения к {DB_PROVIDER} указаны в файле .env")
    sys.exit(1)

# Функция для подключения к базе данных
def connect_to_database():
    if DB_PROVIDER == 'postgres':
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            print(f"✅ Соединение с PostgreSQL установлено успешно!")
            return conn
        except ImportError:
            print("❌ Ошибка: Библиотека psycopg2 не установлена")
            print("Установите её командой: pip install psycopg2-binary")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Ошибка при подключении к PostgreSQL: {e}")
            sys.exit(1)
    else:  # mssql
        try:
            # Настройка JAVA_HOME для Python
            java_home = '/opt/homebrew/opt/openjdk'  # Путь на M1/M2 Mac
            if not os.path.exists(java_home):
                java_home = '/usr/local/opt/openjdk'  # Путь на Intel Mac

            os.environ['JAVA_HOME'] = java_home
            
            import jpype
            import jpype.imports
            
            # Путь к JDBC драйверу
            jdbc_driver_path = './lib/mssql-jdbc.jar'
            
            # Проверка наличия драйвера
            if not os.path.exists(jdbc_driver_path):
                print(f"❌ Ошибка: JDBC драйвер не найден по пути {jdbc_driver_path}")
                print("Скачай драйвер с https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server")
                sys.exit(1)
            
            # Запускаем JVM
            if not jpype.isJVMStarted():
                jvm_path = jpype.getDefaultJVMPath()
                jpype.startJVM(jvm_path, f"-Djava.class.path={jdbc_driver_path}")
            
            # Импортируем Java классы
            from java.sql import DriverManager, SQLException
            
            # JDBC строка подключения
            jdbc_url = f'jdbc:sqlserver://{DB_HOST}:{DB_PORT};databaseName={DB_NAME};trustServerCertificate=true;encrypt=false'
            
            # Создаем соединение
            connection = DriverManager.getConnection(jdbc_url, DB_USER, DB_PASSWORD)
            print(f"✅ Соединение с MSSQL через JDBC установлено успешно!")
            
            return connection
        except Exception as e:
            print(f"❌ Ошибка при подключении к MSSQL: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

# Функция для проверки и создания поля converted_sql
def ensure_converted_sql_column_exists(conn):
    try:
        if DB_PROVIDER == 'postgres':
            cursor = conn.cursor()
            
            # Проверяем существование столбца
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'ppr_calc_algorithm' AND column_name = 'converted_sql'
            """)
            
            if cursor.fetchone() is None:
                print("Создаю столбец converted_sql в таблице PPR_CALC_ALGORITHM...")
                cursor.execute("ALTER TABLE ppr_calc_algorithm ADD COLUMN converted_sql TEXT")
                conn.commit()
                print("✅ Столбец создан успешно")
            else:
                print("Столбец converted_sql уже существует")
                
            cursor.close()
        else:  # mssql
            statement = conn.createStatement()
            
            # Проверяем существование столбца
            result_set = statement.executeQuery("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'PPR_CALC_ALGORITHM' AND column_name = 'CONVERTED_SQL'
            """)
            
            column_exists = result_set.next()
            result_set.close()
            
            if not column_exists:
                print("Создаю столбец CONVERTED_SQL в таблице PPR_CALC_ALGORITHM...")
                statement.execute("ALTER TABLE PPR_CALC_ALGORITHM ADD CONVERTED_SQL NVARCHAR(MAX)")
                print("✅ Столбец создан успешно")
            else:
                print("Столбец CONVERTED_SQL уже существует")
                
            statement.close()
            
    except Exception as e:
        print(f"❌ Ошибка при проверке/создании столбца: {e}")
        if DB_PROVIDER != 'postgres':
            import traceback
            traceback.print_exc()
        raise

# Функция для импорта скриптов в базу данных
def import_scripts_to_database(conn):
    # Получаем список файлов скриптов
    script_files = glob.glob('scripts/exported/*.sql')
    
    if not script_files:
        print("❌ Ошибка: Не найдены SQL-скрипты в папке scripts/exported/")
        return
    
    # Шаблон для извлечения ID из имени файла
    id_pattern = re.compile(r'(\d+)_exp_\d+\.sql')
    
    total_files = len(script_files)
    processed = 0
    failed = 0
    
    try:
        for script_file in script_files:
            # Извлекаем ID из имени файла
            file_name = os.path.basename(script_file)
            match = id_pattern.match(file_name)
            
            if not match:
                print(f"⚠️ Пропуск файла {file_name}: не удалось извлечь ID")
                failed += 1
                continue
            
            a_id = match.group(1)
            
            # Читаем содержимое файла
            with open(script_file, 'r', encoding='utf-8') as f:
                script_content = f.read()
            
            # Обновляем запись в базе данных
            if DB_PROVIDER == 'postgres':
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "UPDATE ppr_calc_algorithm SET converted_sql = %s WHERE a_id = %s",
                        (script_content, a_id)
                    )
                    affected_rows = cursor.rowcount
                    conn.commit()
                    
                    if affected_rows > 0:
                        print(f"✅ Обновлена запись с ID={a_id}")
                        processed += 1
                    else:
                        print(f"⚠️ Запись с ID={a_id} не найдена в базе данных")
                        failed += 1
                except Exception as e:
                    print(f"❌ Ошибка при обновлении записи с ID={a_id}: {e}")
                    failed += 1
                    conn.rollback()
                finally:
                    cursor.close()
            else:  # mssql
                # Для MSSQL используем препарированный statement
                try:
                    prepared_statement = conn.prepareStatement(
                        "UPDATE PPR_CALC_ALGORITHM SET CONVERTED_SQL = ? WHERE A_ID = ?"
                    )
                    
                    prepared_statement.setString(1, script_content)
                    prepared_statement.setInt(2, int(a_id))
                    
                    affected_rows = prepared_statement.executeUpdate()
                    
                    if affected_rows > 0:
                        print(f"✅ Обновлена запись с ID={a_id}")
                        processed += 1
                    else:
                        print(f"⚠️ Запись с ID={a_id} не найдена в базе данных")
                        failed += 1
                        
                    prepared_statement.close()
                except Exception as e:
                    print(f"❌ Ошибка при обновлении записи с ID={a_id}: {e}")
                    import traceback
                    traceback.print_exc()
                    failed += 1
        
        print(f"\n📊 Итоги импорта:")
        print(f"   - Всего файлов: {total_files}")
        print(f"   - Успешно обработано: {processed}")
        print(f"   - Не удалось обработать: {failed}")
        
    except Exception as e:
        print(f"❌ Ошибка при импорте скриптов: {e}")
        if DB_PROVIDER != 'postgres':
            import traceback
            traceback.print_exc()
        raise

def main():
    try:
        # Подключение к базе данных
        conn = connect_to_database()
        
        # Проверка и создание столбца converted_sql
        ensure_converted_sql_column_exists(conn)
        
        # Импорт скриптов
        import_scripts_to_database(conn)
        
        # Закрываем соединение
        if DB_PROVIDER == 'postgres':
            conn.close()
        else:  # mssql
            conn.close()
            if jpype.isJVMStarted():
                jpype.shutdownJVM()
                
        print("✅ Импорт завершен")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if DB_PROVIDER != 'postgres':
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main() 