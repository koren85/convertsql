import psycopg2
import time
import re
import docker
from contextlib import contextmanager
from src.ai_converter import AIConverter

class PostgresTester:
    def __init__(self, config):
        self.config = config
        self.pg_config = config.PG_CONFIG
        self.ai_converter = None  # Ленивая инициализация AI конвертера
        
    @contextmanager
    def get_connection(self):
        """Создает подключение к PostgreSQL"""
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.pg_config['host'],
                port=self.pg_config['port'],
                database=self.pg_config['database'],
                user=self.pg_config['user'],
                password=self.pg_config['password']
            )
            yield connection
        finally:
            if connection is not None:
                connection.close()
    
    def validate_syntax(self, script):
        """
        Проверяет только синтаксис скрипта SQL, без его выполнения и без проверки существования объектов
        """
        result = {
            'success': False,
            'error': None
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Включаем режим check_function_bodies = off
                    cursor.execute("SET check_function_bodies = false;")
                    
                    # Создаем временную схему для тестирования
                    cursor.execute("CREATE SCHEMA IF NOT EXISTS temp_test_schema;")
                    
                    # Создаем временную функцию, которая будет парсить SQL без выполнения
                    cursor.execute("""
                    CREATE OR REPLACE FUNCTION temp_test_schema.validate_sql_syntax(sql_text text)
                    RETURNS boolean AS $$
                    BEGIN
                        BEGIN
                            -- Используем выполнение в отдельной транзакции
                            EXECUTE 'PREPARE stmt AS ' || sql_text;
                            EXECUTE 'DEALLOCATE stmt';
                            RETURN true;
                        EXCEPTION
                            WHEN syntax_error THEN
                                RAISE;
                            WHEN undefined_table THEN
                                -- Игнорируем ошибки о несуществующих таблицах
                                RETURN true;
                            WHEN undefined_column THEN
                                -- Игнорируем ошибки о несуществующих столбцах
                                RETURN true;
                            WHEN OTHERS THEN
                                RAISE;
                        END;
                    END;
                    $$ LANGUAGE plpgsql;
                    """)
                    
                    # Запускаем проверку синтаксиса
                    cursor.execute(f"SELECT temp_test_schema.validate_sql_syntax($${script}$$);")
                    result['success'] = cursor.fetchone()[0]
                    
                    # Удаляем временную функцию
                    cursor.execute("DROP FUNCTION IF EXISTS temp_test_schema.validate_sql_syntax(text);")
        except Exception as e:
            result['error'] = str(e)
            
        return result
    
    def test_script(self, script):
        """
        Тестирует скрипт в PostgreSQL и возвращает результат
        """
        start_time = time.time()
        result = {
            'success': False,
            'error': None,
            'execution_time': None,
            'row_count': None
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Устанавливаем таймаут для запроса
                    cursor.execute(f"SET statement_timeout = {self.config.MAX_EXECUTION_TIME * 1000};")
                    
                    # Выполняем скрипт
                    cursor.execute(script)
                    
                    # Если это SELECT, получаем результаты
                    if cursor.description is not None:
                        rows = cursor.fetchall()
                        result['row_count'] = len(rows)
                    
                    result['success'] = True
        except Exception as e:
            result['error'] = str(e)
        finally:
            result['execution_time'] = time.time() - start_time
            
        return result
    
    def ensure_docker_running(self):
        """
        Проверяет, запущен ли Docker контейнер с PostgreSQL, и запускает его при необходимости
        """
        client = docker.from_env()
        try:
            container = client.containers.get('postgres_test')
            if container.status != 'running':
                container.start()
                # Ждем, пока PostgreSQL загрузится
                time.sleep(5)
        except docker.errors.NotFound:
            print("PostgreSQL container not found. Please run 'docker-compose up -d' first.")
            raise
            
    def fix_script(self, script, error_message, original_script=None, use_ai=True):
        """
        Пытается исправить скрипт на основе сообщения об ошибке.
        Если стандартные методы не помогают, используется нейросеть.
        
        Args:
            script: Текущий (возможно, уже частично сконвертированный) скрипт
            error_message: Сообщение об ошибке из PostgreSQL
            original_script: Исходный скрипт MS SQL (для AI конвертации)
            use_ai: Использовать ли AI для исправления, если обычные методы не помогают
            
        Returns:
            Исправленный скрипт
        """
        # Сначала пробуем стандартные методы исправления
        fixed_script = self._try_standard_fixes(script, error_message)
        
        # Проверяем, помогли ли стандартные исправления
        test_result = self.test_script(fixed_script)
        
        # Если стандартные методы помогли, возвращаем результат
        if test_result['success']:
            return fixed_script
        
        # Если стандартные методы не помогли и разрешено использование AI
        if use_ai and hasattr(self.config, 'USE_AI_CONVERSION') and self.config.USE_AI_CONVERSION:
            try:
                print(f"Стандартные методы исправления не помогли. Пробуем использовать нейросеть...")
                
                # Выбираем исходный скрипт для AI
                ai_input_script = original_script if original_script else script
                
                # Ленивая инициализация AI конвертера
                if self.ai_converter is None:
                    self.ai_converter = AIConverter(self.config)
                
                # Конвертируем с помощью AI
                success, ai_script, message = self.ai_converter.convert_with_ai(
                    ai_input_script, 
                    test_result['error'] or error_message
                )
                
                if success:
                    # Проверяем, работает ли скрипт от AI
                    ai_test = self.test_script(ai_script)
                    if ai_test['success']:
                        print(f"AI успешно исправил скрипт: {message}")
                        return ai_script
                    else:
                        print(f"AI предложил решение, но оно не работает: {ai_test['error']}")
                else:
                    print(f"AI не смог исправить скрипт: {message}")
            
            except Exception as e:
                print(f"Ошибка при использовании AI: {str(e)}")
        
        # Возвращаем скрипт со стандартными исправлениями, если AI не помог
        return fixed_script
    
    def _try_standard_fixes(self, script, error_message):
        """
        Применяет стандартные методы исправления скрипта
        """
        fixed_script = script
        
        # Набор типичных ошибок и их исправлений
        if "syntax error at or near" in error_message:
            # Попытаться идентифицировать проблемное место
            if "TOP" in error_message:
                fixed_script = self._convert_top_to_limit(fixed_script)
            elif "[" in error_message or "]" in error_message:
                fixed_script = fixed_script.replace("[", "\"").replace("]", "\"")
            elif "+=" in error_message:
                fixed_script = fixed_script.replace("+=", " = ")
            elif ";" in error_message:
                fixed_script = fixed_script.replace(";", "")
            elif "DECLARE" in error_message:
                fixed_script = self._fix_variable_declarations(fixed_script)
            elif "TABLE" in error_message:
                fixed_script = self._fix_temp_tables(fixed_script)
                
        elif "column" in error_message and "does not exist" in error_message:
            # Проблема с именем столбца
            match = re.search(r'column "(.*?)" does not exist', error_message)
            if match:
                column_name = match.group(1)
                fixed_script = self._fix_column_name(fixed_script, column_name)
        
        elif "relation" in error_message and "does not exist" in error_message:
            # Проблема с именем таблицы
            match = re.search(r'relation "(.*?)" does not exist', error_message)
            if match:
                table_name = match.group(1)
                fixed_script = self._fix_table_name(fixed_script, table_name)
        
        # Дополнительные правила исправления можно добавлять здесь
                
        return fixed_script
    
    def _convert_top_to_limit(self, script):
        """Преобразует TOP в LIMIT"""
        # Находим TOP и заменяем на эквивалент LIMIT
        match = re.search(r'SELECT\s+TOP\s+(\d+)', script, re.IGNORECASE)
        if match:
            limit = match.group(1)
            script = re.sub(r'SELECT\s+TOP\s+\d+', 'SELECT', script, flags=re.IGNORECASE)
            # Добавляем LIMIT в конец запроса
            script = re.sub(r';?\s*$', f' LIMIT {limit};', script)
        return script
    
    def _fix_column_name(self, script, column_name):
        """Пытается исправить имя столбца"""
        # Добавляем двойные кавычки вокруг имени столбца
        return script.replace(column_name, f'"{column_name}"')
    
    def _fix_table_name(self, script, table_name):
        """Пытается исправить имя таблицы"""
        # Добавляем двойные кавычки вокруг имени таблицы
        pattern = r'\b' + re.escape(table_name) + r'\b'
        return re.sub(pattern, f'"{table_name}"', script)
    
    def _fix_variable_declarations(self, script):
        """Исправляет объявления переменных в PostgreSQL"""
        # В PostgreSQL нет DECLARE для скалярных переменных как в T-SQL
        # Заменяем на DO блок с объявлениями переменных внутри
        
        # Ищем все объявления переменных
        declarations = re.findall(r'DECLARE\s+@(\w+)\s+([^=;]+)(?:\s*=\s*([^;]+))?;', script, re.IGNORECASE)
        
        if declarations:
            # Формируем DO блок
            do_block = "DO $$\nDECLARE\n"
            for var_name, var_type, var_value in declarations:
                do_block += f"    {var_name} {var_type}"
                if var_value:
                    do_block += f" := {var_value.strip()}"
                do_block += ";\n"
            do_block += "BEGIN\n"
            
            # Заменяем оригинальные объявления
            for var_name, var_type, var_value in declarations:
                pattern = r'DECLARE\s+@' + re.escape(var_name) + r'\s+' + re.escape(var_type)
                if var_value:
                    pattern += r'\s*=\s*' + re.escape(var_value)
                pattern += r';'
                script = re.sub(pattern, '', script, flags=re.IGNORECASE)
            
            # Добавляем конец DO блока
            script = do_block + script.strip() + "\nEND $$;"
            
        return script
    
    def _fix_temp_tables(self, script):
        """Исправляет временные таблицы в PostgreSQL"""
        # Заменяем @TempTable на временные таблицы PostgreSQL
        temp_tables = re.findall(r'DECLARE\s+@(\w+)\s+TABLE\s*\(([\s\S]+?)\);', script, re.IGNORECASE)
        
        if temp_tables:
            for table_name, columns in temp_tables:
                # Формируем CREATE TEMP TABLE вместо DECLARE @Table TABLE
                create_stmt = f"CREATE TEMP TABLE {table_name} ({columns});"
                
                # Заменяем оригинальное объявление
                pattern = r'DECLARE\s+@' + re.escape(table_name) + r'\s+TABLE\s*\(' + re.escape(columns) + r'\);'
                script = re.sub(pattern, create_stmt, script, flags=re.IGNORECASE)
                
                # Также заменяем все упоминания @table_name на table_name
                script = re.sub(r'@' + re.escape(table_name), table_name, script)
                
        return script
