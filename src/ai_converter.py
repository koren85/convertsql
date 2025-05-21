"""
Модуль для использования нейросетей в конвертации SQL с MS SQL на PostgreSQL
с автоматической проверкой и итеративным улучшением результатов
"""

import os
import time
import json
import requests
import re
import subprocess
import tempfile
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
from dotenv import load_dotenv
from src.sql_alias_analyzer import SQLAliasAnalyzer

# Загружаем переменные из .env файла
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class AIConverter:
    """
    Класс для конвертации SQL с использованием нейросетей через API
    с автоматической проверкой и исправлением результатов
    """
    
    def __init__(self, config):
        """
        Инициализация с настройками из конфигурации
        
        Args:
            config: Объект конфигурации с настройками для конвертации
        """
        self.config = config
        self.api_keys = {
            'openai': os.getenv('OPENAI_API_KEY', ''),
            'anthropic': os.getenv('ANTHROPIC_API_KEY', ''),
        }
        # Настройки для подключения к тестовой БД PostgreSQL
        self.pg_connection_string = getattr(
            self.config, 
            'PG_CONNECTION_STRING', 
            'postgresql://username:password@localhost:5432/test_db'
        )
        # Значение таймаута для запросов API (по умолчанию 60 секунд)
        self.api_timeout = getattr(self.config, 'API_TIMEOUT', 60)
        print(f"Установлен таймаут API запросов: {self.api_timeout} секунд")
        # Инициализируем анализатор алиасов SQL
        self.alias_analyzer = SQLAliasAnalyzer()
        
    def extract_sql_text(self, script):
        """
        Извлекает текст SQL из различных форматов входящего объекта.
        
        Args:
            script: Входной объект (строка или словарь)
            
        Returns:
            str: Текст SQL-запроса
        """
        if isinstance(script, dict):
            # Если есть ключ 'original', используем его (это наиболее вероятный вариант из парсера)
            if 'original' in script:
                return script['original']
            # Если есть ключ 'text', используем его
            elif 'text' in script:
                return script['text']
            # Если есть ключ 'sql', используем его
            elif 'sql' in script:
                return script['sql']
            # Если есть ключ 'script', используем его
            elif 'script' in script:
                return script['script']
            # Если есть ключ 'statements', пробуем объединить все запросы
            elif 'statements' in script and isinstance(script['statements'], list):
                return '\n'.join(script['statements'])
            # В остальных случаях возвращаем строковое представление словаря
            else:
                print(f"Предупреждение: script - это словарь, но не содержит ожидаемых ключей. Доступные ключи: {list(script.keys())}")
                return str(script)
        else:
            # Если это строка, возвращаем как есть
            return script
        
    def should_skip_conversion(self, script) -> Tuple[bool, str]:
        """
        Проверяет, следует ли пропустить конвертацию скрипта на основе определенных паттернов.
        
        Args:
            script: Содержимое скрипта для проверки (может быть строкой или словарем)
            
        Returns:
            Tuple[bool, str]: (нужно_пропустить, причина_пропуска)
        """
        # Список паттернов, которые указывают на то, что скрипт может быть частью 
        # более крупного скрипта или требует ручной обработки
        patterns = [
            # Скрипт начинается с CASE
            (r'^\s*CASE\s+WHEN', 'Скрипт начинается с CASE WHEN и может быть частью более крупного скрипта'),
            (r'^\s*case', 'Скрипт начинается с CASE (любой регистр) и может быть фрагментом'),
            
            # Содержит специфические функции
            (r'SQL\.equalBeforeInDay', 'Скрипт содержит специфическую функцию SQL.equalBeforeInDay'),
            
            # Содержит конкретные SQL.* паттерны
            (r'SQL\.endMonth', 'Скрипт содержит специфическую функцию SQL.endMonth'),
            (r'SQL\.addYear', 'Скрипт содержит специфическую функцию SQL.addYear'),
            (r'SQL\.getDate', 'Скрипт содержит специфическую функцию SQL.getDate'),
            (r'SQL\.getYearOld', 'Скрипт содержит специфическую функцию SQL.getYearOld'),
            
            # Общий паттерн для всех SQL.* функций
            (r'\{SQL\.[^\}]+\}', 'Скрипт содержит динамические SQL-функции вида {SQL.*}'),
            
            # Содержит специфические параметры
            (r'ALG\.[a-zA-Z]+', 'Скрипт содержит динамические параметры вида ALG.*'),
            
            # Вызовы хранимых процедур из схемы public
            (r'public\.[a-zA-Z0-9_]+\s*\(', 'Скрипт содержит вызов хранимой процедуры public.*'),
            
            # Вызов специфичной функции check_documents_in_personal_card
            (r'check_documents_in_personal_card\s*\(', 'Скрипт содержит вызов специфической функции check_documents_in_personal_card'),
            
            # Другие шаблоны, которые можно добавить позже...
            # (r'другой_паттерн', 'другое_сообщение'),
        ]
        
        # Извлекаем текст скрипта
        script_text = self.extract_sql_text(script)

        # Проверяем каждый паттерн
        for pattern, message in patterns:
            if re.search(pattern, script_text, re.IGNORECASE):
                return True, message
        
        # Ничего не найдено
        return False, ""
        
    def convert_with_ai(self, original_script, error_message: str = None, 
                         max_iterations: int = 3) -> Tuple[bool, str, str]:
        """
        Конвертирует скрипт используя нейросеть с возможностью нескольких итераций
        
        Args:
            original_script: Исходный MS SQL скрипт (может быть строкой или словарем)
            error_message: Сообщение об ошибке из PostgreSQL, если есть
            max_iterations: Максимальное количество итераций для проверки и исправления
            
        Примечание:
            Таймаут запросов к API может быть настроен через конфигурацию (API_TIMEOUT)
            или через параметр командной строки --timeout.
            Для больших скриптов (>4000 строк) рекомендуется увеличить таймаут до 180-300 секунд.
            
        Returns:
            Tuple[bool, str, str]: (успех, сконвертированный скрипт, сообщение)
        """
        try:
            # Проверяем, нужно ли пропустить конвертацию
            should_skip, skip_reason = self.should_skip_conversion(original_script)
            if should_skip:
                print(f"\n⚠️ Скрипт требует ручной обработки: {skip_reason}")
                # Извлекаем текст скрипта
                script_text = self.extract_sql_text(original_script)
                return False, script_text, f"Скрипт требует ручной обработки: {skip_reason}"
            
            # Извлекаем текст скрипта
            script_text = self.extract_sql_text(original_script)
            
            # Определяем какой API использовать из конфигурации
            ai_provider = getattr(self.config, 'AI_PROVIDER', 'openai').lower()
            
            print(f"\n--- Начинаем конвертацию с помощью {ai_provider.upper()} ---")
            
            # Первичная конвертация
            if ai_provider == 'openai':
                success, converted_script, message = self._convert_with_openai(script_text, error_message)
            elif ai_provider == 'anthropic':
                success, converted_script, message = self._convert_with_anthropic(script_text, error_message)
            else:
                return False, script_text, f"Неизвестный провайдер AI: {ai_provider}"
            
            if not success:
                return False, script_text, message
            
            print(f"✅ Первичная конвертация успешно выполнена")
            
            # Итеративное улучшение скрипта с проверкой его работоспособности
            iteration = 0
            current_script = converted_script
            
            while iteration < max_iterations:
                print(f"\n--- Итерация {iteration+1}/{max_iterations} ---")
                
                # Проверяем работоспособность скрипта
                script_works, error = self._test_script_in_postgres(current_script)

                # Проверка на type mismatch — если да, сразу выходим
                type_mismatch_patterns = [
                    "could not identify an equality operator",
                    "operator does not exist",
                    "cannot compare",
                    "ОШИБКА:  оператор не существует",
                    "types ",
                    "cannot be matched"
                ]
                def is_type_mismatch_error(error_msg):
                    return any(pat in error_msg for pat in type_mismatch_patterns)
                if not script_works and error and is_type_mismatch_error(error):
                    print("⛔️ Несовпадение типов в JOIN/WHERE — не отправляю на доработку, останавливаю конвертацию!")
                    return False, current_script, error

                if script_works:
                    print(f"✅ Скрипт успешно проверен в PostgreSQL (итерация {iteration+1})")
                    return True, current_script, "Успешно сконвертировано и проверено в PostgreSQL"
                
                # Если скрипт не работает, пытаемся исправить с помощью AI
                print(f"Итерация {iteration+1}: Скрипт содержит ошибки, пытаемся исправить")
                print(f"Ошибка: {error}")
                print(f"🔄 Отправляем скрипт на доработку...")
                
                # Конвертируем снова, но с сообщением об ошибке
                if ai_provider == 'openai':
                    success, fixed_script, message = self._convert_with_openai(current_script, error)
                else:
                    success, fixed_script, message = self._convert_with_anthropic(current_script, error)
                
                if not success:
                    # Если не удалось исправить, возвращаем последнюю версию и сообщение
                    return False, current_script, f"Не удалось исправить скрипт: {message}"
                
                current_script = fixed_script
                iteration += 1
            
            # Если после всех итераций скрипт все еще не работает, возвращаем последнюю версию
            return False, current_script, f"Достигнуто максимальное количество итераций ({max_iterations}), скрипт может содержать ошибки"
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            # Извлекаем текст скрипта
            script_text = self.extract_sql_text(original_script)
            return False, script_text, f"Ошибка при конвертации: {str(e)}"
    
    def _test_script_in_postgres(self, script: str) -> Tuple[bool, str]:
        """
        Тестирует скрипт в PostgreSQL
        
        Args:
            script: SQL скрипт для проверки
            
        Returns:
            Tuple[bool, str]: (успех, сообщение об ошибке если есть)
        """
        use_real_db = getattr(self.config, 'USE_REAL_DB_TESTING', True)
        
        # Сохраняем оригинальный скрипт и параметры
        original_script = script
        
        # Определяем, использовать ли улучшенный парсер
        use_improved_parser = getattr(self.config, 'USE_IMPROVED_PARSER', True)
        
        if use_improved_parser:
            try:
                # Используем улучшенный парсер с анализом контекста
                from src.parser import SQLParser
                parser = SQLParser(self.config)
                # Анализируем алиасы таблиц перед заменой параметров
                table_aliases = self.alias_analyzer.get_table_aliases(script)
                print("\n✅ Анализ алиасов таблиц:")
                for table, aliases in table_aliases.items():
                    if aliases:
                        print(f"  {table}: {', '.join(aliases)}")
                    else:
                        print(f"  {table}: Нет алиаса")
                        
                script = parser.replace_params(script)
                print("✅ Параметры заменены с использованием улучшенного анализа контекста")
            except Exception as e:
                print(f"❌ Ошибка при использовании улучшенного парсера: {str(e)}")
                print("⚠️ Возвращаемся к старому методу замены параметров")
                use_improved_parser = False
        
        # Если не используем улучшенный парсер, применяем старую логику
        if not use_improved_parser:
            # Старая логика замены параметров
            # Перед тестированием, найдем все параметры в скрипте
            param_patterns = [
                r'\{params\.([^}]+)\}',           # {params.someValue}
                r'\{([^}]+)\}',                   # {someValue}
                r'default_params\.([a-zA-Z0-9_]+)', # default_params.someValue
                r'params\.([a-zA-Z0-9_]+)'         # params.someValue
            ]
            
            # Простая замена всех параметров на тестовые значения
            for pattern in param_patterns:
                matches = re.findall(pattern, script)
                for param_name in matches:
                    test_value = '1'  # Значение по умолчанию для ID
                    
                    # Подбираем тип значения в зависимости от названия параметра
                    if 'id' in param_name.lower() or 'ouid' in param_name.lower():
                        test_value = '1'  # ID как число
                    elif 'date' in param_name.lower():
                        test_value = "'2023-01-01'::timestamp"  # Дата как строка
                    elif 'string' in param_name.lower() or 'text' in param_name.lower() or 'name' in param_name.lower():
                        test_value = "'test'"  # Строковое значение
                    
                    # Формируем паттерн замены в зависимости от формата параметра
                    if pattern == param_patterns[0]:
                        replace_pattern = f"{{params.{param_name}}}"
                    elif pattern == param_patterns[1]:
                        replace_pattern = f"{{{param_name}}}"
                    elif pattern == param_patterns[2]:
                        replace_pattern = f"default_params.{param_name}"
                    else:
                        replace_pattern = f"params.{param_name}"
                    
                    # Заменяем параметр
                    print(f"Заменяем параметр '{replace_pattern}' на '{test_value}'")
                    script = script.replace(replace_pattern, test_value)
        
        # Проверяем, остались ли параметры
        remaining_patterns = [
            r'\{params\.([^}]+)\}',
            r'\{([^}]+)\}',
            r'default_params\.([a-zA-Z0-9_]+)',
            r'params\.([a-zA-Z0-9_]+)'
        ]
        for pattern in remaining_patterns:
            if re.search(pattern, script):
                print(f"⚠️ Внимание: в скрипте все еще есть параметры: {re.findall(pattern, script)}")
        
        # Проверяем с тестовыми значениями
        if use_real_db:
            success, error = self._test_in_real_postgres(script)
        else:
            success, error = self._test_with_syntax_checking(script)
        
        # Если ошибка несовпадения типов — выводим traceback и сразу возвращаем ошибку
        type_mismatch_patterns = [
            "could not identify an equality operator",
            "operator does not exist",
            "cannot compare",
            "ОШИБКА:  оператор не существует",
            "types ",
            "cannot be matched"
        ]
        def is_type_mismatch_error(error_msg):
            return any(pat in error_msg for pat in type_mismatch_patterns)
        if not success and error and is_type_mismatch_error(error):
            import traceback
            print("❌ Обнаружена ошибка несовпадения типов в JOIN/WHERE!")
            traceback.print_exc()
            return False, error
        if success:
            return True, ""
        return False, error
        
    def _test_in_real_postgres(self, script: str) -> Tuple[bool, str]:
        """
        Тестирует скрипт в реальной базе данных PostgreSQL
        
        Args:
            script: SQL скрипт для выполнения
            
        Returns:
            Tuple[bool, str]: (успех, сообщение об ошибке если есть)
        """
        # Создаем временный файл со скриптом
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
            temp_file.write(script)
            temp_file_path = temp_file.name
        
        try:
            # Проверяем наличие psql
            try:
                subprocess.run(["psql", "--version"], capture_output=True, check=True)
                print(f"✅ psql найден и доступен")
            except (subprocess.SubprocessError, FileNotFoundError):
                print("⚠️ psql не найден. Используем синтаксический анализ вместо реального тестирования.")
                return self._test_with_syntax_checking(script)
            
            # Выводим для отладки, с какой строкой подключения работаем
            print(f"🔄 Подключение к PostgreSQL с использованием: {self.pg_connection_string.replace(self.config.PG_CONFIG['password'], '****')}")
            
            # Запускаем psql для выполнения скрипта
            result = subprocess.run(
                ["psql", self.pg_connection_string, "-f", temp_file_path, "-v", "ON_ERROR_STOP=1"],
                capture_output=True,
                text=True,
                timeout=getattr(self.config, 'MAX_EXECUTION_TIME', 30)
            )
            
            if result.returncode == 0:
                print(f"✅ SQL скрипт успешно выполнен в PostgreSQL")
                return True, ""
            else:
                # Если ошибка связана с подключением, используем синтаксический анализ
                if "connection to server" in result.stderr and "failed" in result.stderr:
                    print(f"⚠️ Не удалось подключиться к PostgreSQL: {result.stderr}")
                    print(f"Используем синтаксический анализ вместо реального тестирования.")
                    return self._test_with_syntax_checking(script)
                # Если ошибка несовпадения типов — выводим traceback и сразу возвращаем ошибку
                type_mismatch_patterns = [
                    "could not identify an equality operator",
                    "operator does not exist",
                    "cannot compare",
                    "ОШИБКА:  оператор не существует",
                    "types ",
                    "cannot be matched"
                ]
                def is_type_mismatch_error(error_msg):
                    return any(pat in error_msg for pat in type_mismatch_patterns)
                if is_type_mismatch_error(result.stderr):
                    import traceback
                    print("❌ Обнаружена ошибка несовпадения типов в JOIN/WHERE!")
                    traceback.print_exc()
                    return False, result.stderr
                else:
                    print(f"❌ Ошибка при выполнении SQL: {result.stderr}")
                    return False, result.stderr
        except subprocess.TimeoutExpired:
            print(f"⚠️ Превышен таймаут выполнения SQL ({getattr(self.config, 'MAX_EXECUTION_TIME', 30)} секунд)")
            return False, f"Превышен таймаут выполнения ({getattr(self.config, 'MAX_EXECUTION_TIME', 30)} секунд)"
        except Exception as e:
            print(f"❌ Ошибка при тестировании SQL: {str(e)}")
            return False, str(e)
        finally:
            # Удаляем временный файл
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    
    def _test_with_syntax_checking(self, script: str) -> Tuple[bool, str]:
        """
        Проверяет синтаксис SQL без реального выполнения
        Использует простые регулярные выражения для обнаружения типичных проблем
        
        Args:
            script: SQL скрипт для проверки (с уже замененными параметрами)
            
        Returns:
            Tuple[bool, str]: (успех, сообщение об ошибке если есть)
        """
        # Проверяем наличие типичных проблем
        problems = []
        
        # Проверка на смешивание citext и integer типов в COALESCE
        coalesce_pattern = r"COALESCE\s*\(\s*([^,]+),\s*([^)]+)\)"
        for match in re.finditer(coalesce_pattern, script):
            arg1, arg2 = match.groups()
            # Примитивное определение типов по содержимому
            if (('::citext' in arg1 or '::CITEXT' in arg1) and 
                ('::integer' in arg2 or '::INTEGER' in arg2 or arg2.strip().isdigit())):
                problems.append(f"COALESCE с несовместимыми типами: {match.group(0)}")
            
            # Проверка смешивания строк и чисел
            if (("'" in arg1 and not "'" in arg2 and arg2.strip().replace('.', '', 1).isdigit()) or
                (not "'" in arg1 and arg1.strip().replace('.', '', 1).isdigit() and "'" in arg2)):
                problems.append(f"COALESCE смешивает текст и числа без приведения типов: {match.group(0)}")
        
        # Проверка на другие типичные проблемы
        type_cast_pattern = r"SET\s+\w+\s*=\s*'[^']*'::(\w+)"
        for match in re.finditer(type_cast_pattern, script):
            problems.append(f"Неправильное приведение типов в SET: {match.group(0)}")
        
        # Проверка на возможные проблемы с типами в условиях WHERE
        where_pattern = r"WHERE\s+.*?([^\s]+(?:::(?:ci)?text|::varchar))\s*=\s*(\d+)(?!\s*::)"
        for match in re.finditer(where_pattern, script, re.IGNORECASE):
            problems.append(f"Возможно несоответствие типов в WHERE: {match.group(0)}")
        
        # Если найдены проблемы, возвращаем False и список проблем
        if problems:
            return False, "\n".join(problems)
        
        return True, ""
    
    def _convert_with_openai(self, original_script: str, error_message: str = None) -> Tuple[bool, str, str]:
        """
        Конвертирует скрипт используя OpenAI API
        
        Args:
            original_script: Исходный SQL скрипт
            error_message: Сообщение об ошибке, если есть
            
        Returns:
            Tuple[bool, str, str]: (успех, сконвертированный скрипт, сообщение)
        """
        api_key = self.api_keys.get('openai')
        if not api_key:
            return False, original_script, "API ключ OpenAI не найден. Проверьте файл .env или переменную окружения OPENAI_API_KEY."
        
        model = getattr(self.config, 'OPENAI_MODEL', 'gpt-4') 
        temperature = getattr(self.config, 'AI_TEMPERATURE', 0.1)
        max_tokens = getattr(self.config, 'AI_MAX_TOKENS', 4000)
        
        # Формируем промпт для модели с улучшенным описанием для типов данных
        prompt = self._create_improved_prompt(original_script, error_message)
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        data = {
            "model": model,
            "messages": [
                {"role": "system", "content": self._get_system_prompt()},
                {"role": "user", "content": prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=self.api_timeout  # Используем настраиваемый таймаут
            )
            
            if response.status_code != 200:
                return False, original_script, f"Ошибка API OpenAI: {response.status_code} - {response.text}"
            
            response_data = response.json()
            converted_script = response_data['choices'][0]['message']['content']
            
            # Извлекаем SQL из ответа (может содержать пояснения)
            converted_script = self._extract_sql_from_response(converted_script)
            
            # Постобработка результата
            converted_script = self._post_process_sql(converted_script)
            
            return True, converted_script, "Успешно сконвертировано с помощью OpenAI"
            
        except requests.exceptions.Timeout:
            return False, original_script, f"Превышен таймаут запроса к OpenAI API ({self.api_timeout} секунд). Попробуйте увеличить таймаут с помощью параметра --timeout."
        except Exception as e:
            return False, original_script, f"Ошибка при запросе к OpenAI: {str(e)}"
    
    def _convert_with_anthropic(self, original_script: str, error_message: str = None) -> Tuple[bool, str, str]:
        """
        Конвертирует скрипт используя Anthropic API
        
        Args:
            original_script: Исходный SQL скрипт
            error_message: Сообщение об ошибке, если есть
            
        Returns:
            Tuple[bool, str, str]: (успех, сконвертированный скрипт, сообщение)
        """
        api_key = self.api_keys.get('anthropic')
        if not api_key:
            return False, original_script, "API ключ Anthropic не найден. Проверьте файл .env или переменную окружения ANTHROPIC_API_KEY."
        
        model = getattr(self.config, 'ANTHROPIC_MODEL', 'claude-3-sonnet-20240229')
        temperature = getattr(self.config, 'AI_TEMPERATURE', 0.1)
        max_tokens = getattr(self.config, 'AI_MAX_TOKENS', 4000)
        
        # Формируем промпт для модели с улучшенным описанием для типов данных
        prompt = self._create_improved_prompt(original_script, error_message)
        
        # Заголовки для Anthropic API
        headers = {
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
            "x-api-key": api_key
        }
        
        # Печатаем первые 10 символов ключа для отладки
        key_preview = api_key[:10] + "..." if len(api_key) > 10 else api_key
        print(f"Используем ключ API Anthropic (первые символы): {key_preview}")
        
        data = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": self._get_system_prompt(),
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=data,
                timeout=self.api_timeout  # Используем настраиваемый таймаут
            )
            
            print(f"Код ответа от Anthropic API: {response.status_code}")
            
            if response.status_code != 200:
                return False, original_script, f"Ошибка API Anthropic: {response.status_code} - {response.text}"
            
            response_data = response.json()
            converted_script = response_data['content'][0]['text']
            
            # Извлекаем SQL из ответа (может содержать пояснения)
            converted_script = self._extract_sql_from_response(converted_script)
            
            # Постобработка результата
            converted_script = self._post_process_sql(converted_script)
            
            return True, converted_script, "Успешно сконвертировано с помощью Anthropic Claude"
            
        except requests.exceptions.Timeout:
            return False, original_script, f"Превышен таймаут запроса к Anthropic API ({self.api_timeout} секунд). Попробуйте увеличить таймаут с помощью параметра --timeout."
        except Exception as e:
            return False, original_script, f"Ошибка при запросе к Anthropic: {str(e)}"
    
    def _get_system_prompt(self) -> str:
        """
        Возвращает системный промпт для моделей
        
        Returns:
            str: Системный промпт с инструкциями для модели
        """
        return """
Ты - эксперт по миграции SQL-скриптов с MS SQL на PostgreSQL.
Твоя задача - правильно конвертировать скрипты с учетом всех особенностей обеих систем.

### ВАЖНО! СТРОГИЙ ЗАПРЕТ!
* НИКОГДА НЕ ДОБАВЛЯЙ преобразования типов (::TEXT и подобные) в условиях JOIN!
* ОСТАВЛЯЙ условия JOIN в точности как в оригинале!
* Такие преобразования в JOIN излишни и ухудшают производительность!
* Например, НЕЛЬЗЯ преобразовывать "ON t1.id = t2.id" в "ON t1.id::TEXT = t2.id::TEXT"!

Особенно важно:
1. Корректно обрабатывать типы данных и их совместимость
2. Правильно преобразовывать функции и синтаксис
3. Учитывать различия в работе с временными таблицами, схемами и т.д.
4. Учитывать проблемы с COALESCE где могут быть несовместимые типы данных (citext и integer)
5. Правильно преобразовывать функции работы с датами
6. Гарантировать, что вывод будет синтаксически и логически верным SQL-кодом для PostgreSQL
7. Если в postgres нет такой функции как в MS SQL (например DATEDIFF), то замени ее на эквивалентную функцию или выражение в postgres (DATE_PART)
8. Не используй при конвертации функцию вычисления дат EPOCH вместо неё используй (DATE_PART)
9. Указывай в комментариях какое выражение было в оригинальном скрипте и какое получилось после конвертации
10. НЕ добавлять лишние преобразования типов в условиях JOIN, если в оригинале их не было
11. Если встречается выражение dateadd(day, 1-day(X), X), то замени его на DATE_TRUNC('month', X)::timestamp (например, dateadd(day, 1-day(wan.A_DATE_REG), wan.A_DATE_REG) -> DATE_TRUNC('month', wan.A_DATE_REG)::timestamp)
12. Все выражения DATEDIFF(DAY, X, Y) и DATEDIFF('day', X, Y) конвертируй в DATE_PART('day', Y - X). Если X или Y содержит ISNULL(...), замени ISNULL на COALESCE.
13. Если требуется обнулить время у даты (например, CONVERT(DATETIME, CONVERT(VARCHAR(10), ...))), всегда используй DATE_TRUNC('day', ...) без ::timestamp, ::date, TO_TIMESTAMP и других преобразований. Не добавляй ::timestamp к параметрам, если задача — оставить только дату.
14. Если встречается выражение ROUND(X, Y), то учти что аргумент X должен быть приведён к типу numeric.
15. Если встречается выражение ROUND(CAST(X as NUMERIC(Y,Z),2), то учти, что правильное выражение должно быть ROUND(X)::numeric((Y,Z), 2), аргумент X должен быть приведён к типу numeric.


Если в запросе есть COALESCE с разными типами, явно указывай приведение типов.
В PostgreSQL типы должны совпадать в таких функциях, как COALESCE, NULLIF, CASE и т.д.

Помни про следующие особенности PostgreSQL:
1. Типы citext и integer несовместимы и не могут быть автоматически приведены
2. В выражениях сравнения (WHERE a = b) типы также должны совпадать
3. В PostgreSQL нет неявного преобразования между числами и строками
4. Операторы CAST и :: могут использоваться для явного преобразования типов
5. Условия JOIN (ON t1.id = t2.id) не требуют явного приведения типов, если соединяемые поля имеют совместимые типы

В каждом месте, где ты изменяешь оригинальный SQL-код (например, преобразуешь функцию, тип данных, синтаксис, логику), обязательно вставляй комментарий в стиле:
  /* [изменено]: <коротко что было и что стало, или почему изменено> */
Комментарии должны быть прямо в теле SQL, на той же строке или перед изменённой строкой.
Не пиши никаких объяснений вне SQL-кода, только комментарии внутри кода.
Если строка не изменялась — не добавляй комментарий.
Пример:
    SELECT COALESCE(field, 0) /* [изменено]: ISNULL(field, 0) -> COALESCE(field, 0) */
    /* [изменено]: TOP 10 -> LIMIT 10 */
    LIMIT 10;

Верни только итоговый SQL-код с комментариями изменений.

"""
    
    def _create_improved_prompt(self, original_script: str, error_message: str = None) -> str:
        """
        Создает улучшенный промпт для нейросети
        
        Args:
            original_script: Исходный SQL скрипт
            error_message: Сообщение об ошибке, если есть
            
        Returns:
            str: Улучшенный промпт для модели
        """
        prompt = """
Я хочу конвертировать скрипт из MS SQL в PostgreSQL. Пожалуйста, произведи конвертацию, учитывая следующие правила:

### Обязательные преобразования:
1. Преобразуй синтаксис MS SQL в синтаксис PostgreSQL
2. Убери квадратные скобки для идентификаторов
3. Замени все найденные функции MS SQL на эквиваленты PostgreSQL, например (ISNULL -> COALESCE, GETDATE() -> CURRENT_TIMESTAMP и т.д.)
4. Преобразуй типы данных (NVARCHAR -> VARCHAR, DATETIME -> TIMESTAMP и т.д.)
5. Замени TOP на LIMIT
6. Адаптируй временные таблицы (# -> с использованием TEMP или WITH)
7. Устрани другие проблемы совместимости
8. Если в postgres нет такой функции как в MS SQL (например DATEDIFF), то замени ее на эквивалентную функцию или выражение в postgres (DATE_PART)
9. Не используй при конвертации функцию вычисления дат EPOCH вместо неё используй (DATE_PART)
10. Указывай в комментариях какое выражение было в оригинальном скрипте и какое получилось после конвертации


### Особое внимание к типам данных:
1. Внимательно проверяй совместимость типов в функциях COALESCE, CASE и при сравнении
2. Если в COALESCE смешиваются разные типы (например, citext и integer), используй явное приведение с помощью CAST или ::
3. При работе с датами используй соответствующие функции PostgreSQL вместо MS SQL функций
4. Когда используешь тип citext, убедись что все операции с ним используют совместимые типы
5. В PostgreSQL нет неявного преобразования между числами и строками, поэтому добавляй явное преобразование
6. НЕ ДОБАВЛЯЙ явные приведения типов в условиях JOIN, если в оригинальном скрипте их не было (например, НЕ преобразовывай "ON t1.id = t2.id" в "ON t1.id::TEXT = t2.id::TEXT")
7. Сохраняй оригинальные условия JOIN, если они не вызывают ошибок совместимости типов

### Примеры правильных преобразований:
1. COALESCE(field::citext, 'default'::citext) вместо COALESCE(field, 'default')
2. COALESCE(numeric_field, 0) вместо COALESCE(numeric_field, '0')
3. CAST('2023-01-01' AS TIMESTAMP) вместо '2023-01-01'::TIMESTAMP в операторах SET
4. field::text = '123' вместо field = 123 (когда field - текстовый тип)
5. Параметры в фигурных скобках {{params.someValue}} оставлять без изменений
6. WHERE numeric_field = 123::numeric вместо WHERE numeric_field = '123'
7. Параметры вида {{params.someValue}} оставляй без изменений
8. ON t1.id = t2.id оставлять как есть, без добавления ::TEXT

### Известные проблемные паттерны:
1. COALESCE(text_field, numeric_value) - несовместимые типы
2. text_field = numeric_value - требуется привести типы к одному
3. Неправильное использование ::TIMESTAMP в SET операторах - используй CAST() вместо ::
4. Временные таблицы - замени # на корректный синтаксис PostgreSQL
5. НЕ ДОБАВЛЯЙ лишние преобразования типов в условиях JOIN (t1.id::TEXT = t2.id::TEXT)

### ПОВТОРЕНИЕ СТРОГОГО ЗАПРЕТА!
* НИКОГДА НЕ ДОБАВЛЯЙ ::TEXT или CAST AS TEXT в условиях ON для JOIN!
* НИКОГДА НЕ ИЗМЕНЯЙ условия JOIN по сравнению с оригиналом!
* В PostgreSQL условия JOIN не требуют явного приведения типов!
* Сохраняй условия типа "ON a.id = b.id" в точности как есть!

Вставляй комментарии в стиле /* было: ... стало: ... */ или /* изменено: ... */ прямо в SQL, но не пиши никаких текстовых объяснений вне кода.

Вот исходный MS SQL скрипт для конвертации:
```sql
{0}
```
""".format(original_script)

        # Если есть сообщение об ошибке, добавляем его с подробностями
        if error_message:
            prompt += f"""
При попытке выполнить скрипт в PostgreSQL, возникла следующая ошибка:
```
{error_message}
```

Пожалуйста, тщательно исправь скрипт, чтобы устранить эту ошибку, уделяя особое внимание:
1. Преобразованию типов данных, особенно в выражениях COALESCE, CASE и при сравнении
2. Корректной обработке типов citext, integer, varchar и других типов
3. Правильной работе с временными таблицами и переменными
4. Синтаксису операторов PostgreSQL
5. Параметры в формате {{params.someValue}} или {{someValue}} должны остаться без изменений

Если ошибка связана с типами данных (например, "COALESCE types citext and integer cannot be matched"), 
убедись что все аргументы функций имеют совместимые типы, используя явные преобразования типов.
"""

        return prompt
    
    def _extract_sql_from_response(self, response: str) -> str:
        """
        Извлекает SQL-код из ответа нейросети
        
        Args:
            response: Полный ответ от нейросети
            
        Returns:
            str: Извлеченный SQL-код
        """
        # Удалим все маркеры обратных кавычек, если они находятся в начале или конце строки
        response = re.sub(r'^```sql\s*\n', '', response, flags=re.MULTILINE)
        response = re.sub(r'^```\s*\n', '', response, flags=re.MULTILINE)
        response = re.sub(r'\n```\s*$', '', response, flags=re.MULTILINE)

        # Пытаемся найти SQL между тройными обратными кавычками
        sql_pattern = r"```sql\s*([\s\S]*?)\s*```"
        sql_match = re.search(sql_pattern, response)
        
        if sql_match:
            sql_code = sql_match.group(1).strip()
        else:
            # Если не нашли в формате ```sql, ищем просто между тройными кавычками
            sql_pattern = r"```\s*([\s\S]*?)\s*```"
            sql_match = re.search(sql_pattern, response)
            
            if sql_match:
                sql_code = sql_match.group(1).strip()
            else:
                # Если и так не нашли, возвращаем весь ответ, предполагая, что это чистый SQL
                sql_code = response.strip()
        
        # Финальная проверка, чтобы убедиться, что в тексте не остались маркеры ```
        sql_code = re.sub(r'```sql|```', '', sql_code)

        return sql_code
    
    def _post_process_sql(self, sql_code: str) -> str:
        """
        Постобработка SQL после конвертации для исправления типичных проблем
        
        Args:
            sql_code: Сконвертированный SQL-код
            
        Returns:
            str: Обработанный SQL-код с исправлениями
        """
        # Исправляем неправильное использование ::TIMESTAMP в операторах SET
        sql_code = re.sub(
            r"(SET\s+\w+\s*=\s*'[^']*')::TIMESTAMP", 
            r"SET \1::TIMESTAMP", 
            sql_code
        )
        
        # После всех замен: убираем ::text/::varchar у COALESCE, если оба аргумента — int-поля или числа
        int_fields = ['a_status', 'status', 'petitionid', 'id', 'ouid', 'from_id', 'to_id', 'a_ouid', 'a_id', 'a_count_all_work_day']
        float_fields = ['a_regioncoeff']
        def is_int_field(arg):
            name = arg.split('.')[-1].lower()
            return name in int_fields
        def is_float_field(arg):
            name = arg.split('.')[-1].lower()
            return name in float_fields
        def clean_coalesce_casts(m):
            arg1 = m.group(1).strip()
            arg2 = m.group(2).strip()
            arg1_is_number = arg1.replace('.', '', 1).isdigit()
            arg2_is_number = arg2.replace('.', '', 1).isdigit()
            arg1_is_float = bool(re.fullmatch(r"\d+\.\d+", arg1.strip())) or is_float_field(arg1)
            arg2_is_float = bool(re.fullmatch(r"\d+\.\d+", arg2.strip())) or is_float_field(arg2)
            # Если оба — int/float-поля или числа, убираем ::text/::varchar
            if ((is_int_field(arg1) or arg1_is_number or arg1_is_float) and
                (is_int_field(arg2) or arg2_is_number or arg2_is_float)):
                arg1_clean = re.sub(r'::(text|varchar|citext)', '', arg1, flags=re.IGNORECASE)
                arg2_clean = re.sub(r'::(text|varchar|citext)', '', arg2, flags=re.IGNORECASE)
                return f"COALESCE({arg1_clean}, {arg2_clean})"
            return m.group(0)
        sql_code = re.sub(
            r"COALESCE\s*\(\s*([^,]+?)\s*,\s*([^\)]+?)\s*\)",
            clean_coalesce_casts,
            sql_code
        )
        
        # Исправляем сравнения в WHERE
        def fix_where_comparison(match):
            full_match = match.group(0)
            field = match.group(1)
            value = match.group(2)
            
            # Если поле имеет тип text или varchar, а значение числовое
            if ('::text' in field.lower() or '::varchar' in field.lower() or '::citext' in field.lower()) and value.isdigit():
                return f"{field} = '{value}'"
            
            return full_match
        
        sql_code = re.sub(
            r"([\w.]+(?:::(?:ci)?text|::varchar))\s*=\s*(\d+)(?!\s*::)", 
            fix_where_comparison, 
            sql_code
        )
        
        # Исправляем случаи, когда строки сравниваются с числами
        def fix_string_number_comparison(match):
            full_match = match.group(0)
            string_lit = match.group(1)
            number = match.group(2)
            
            return f"{string_lit} = '{number}'"
        
        sql_code = re.sub(
            r"('[^']*')\s*=\s*(\d+)(?!\s*::)", 
            fix_string_number_comparison, 
            sql_code
        )

        # Список имен полей, для которых не нужно добавлять приведение типов
        id_fields = ['ouid', 'a_ouid', 'a_id', 'id', 'from_id', 'to_id', 'a_mspholder', 'documentstype']
        
        # Добавляем явное приведение типов в CASE выражениях
        def fix_case_expression(match):
            full_match = match.group(0)
            case_body = match.group(1)

            # Собираем все THEN/ELSE значения
            then_else_values = re.findall(r"THEN\s+([^\s]+)|ELSE\s+([^\s]+)", case_body)
            values = [v[0] or v[1] for v in then_else_values]
            has_number = any(re.fullmatch(r"\d+", v) for v in values if v is not None)
            has_string = any("'" in v for v in values if v is not None)
            has_null = any(v.upper() == 'NULL' for v in values if v is not None)

            # Если есть и строки, и числа — приводим числа к ::text
            if has_string and has_number:
                def repl(m):
                    val = m.group(1)
                    if re.fullmatch(r"\d+", val):
                        return f"THEN {val}::text"
                    return m.group(0)
                case_body = re.sub(r"THEN\s+(\d+)", repl, case_body)
                def repl_else(m):
                    val = m.group(1)
                    if re.fullmatch(r"\d+", val):
                        return f"ELSE {val}::text"
                    return m.group(0)
                case_body = re.sub(r"ELSE\s+(\d+)", repl_else, case_body)
                return f"CASE {case_body} END"
            # Если все числа или NULL — ничего не делаем
            return full_match
        
        sql_code = re.sub(
            r"CASE\s+(.*?)\s+END", 
            fix_case_expression, 
            sql_code,
            flags=re.DOTALL
        )
        
        # Исправляем случаи, когда COALESCE с текстовым результатом сравнивается с числом
        coalesce_comparison_pattern = r"(COALESCE\s*\([^)]*?::text[^)]*\))\s*=\s*(\d+)(?!\s*::)"
        sql_code = re.sub(
            coalesce_comparison_pattern,
            lambda m: f"{m.group(1)} = '{m.group(2)}'",
            sql_code
        )
        
        # Приводим DATEDIFF(DAY, ...) к DATEDIFF('day', ...)
        sql_code = re.sub(r"DATEDIFF\s*\(\s*DAY\s*,", "DATEDIFF('day',", sql_code, flags=re.IGNORECASE)
        
        # DATEDIFF(DAY, X, Y) и DATEDIFF('day', X, Y) -> DATE_PART('day', Y - X), ISNULL -> COALESCE
        def datediff_to_datepart(m):
            x = m.group(1).strip()
            y = m.group(2).strip()
            x = re.sub(r'ISNULL\s*\(', 'COALESCE(', x, flags=re.IGNORECASE)
            y = re.sub(r'ISNULL\s*\(', 'COALESCE(', y, flags=re.IGNORECASE)
            return f"DATE_PART('day', {y} - {x})"
        sql_code = re.sub(
            r"DATEDIFF\s*\(\s*(?:DAY|'day')\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
            datediff_to_datepart,
            sql_code,
            flags=re.IGNORECASE
        )

        # YEAR(X) -> EXTRACT(YEAR FROM X::timestamp)
        sql_code = re.sub(
            r"YEAR\s*\(\s*([^\)]+)\s*\)",
            r"EXTRACT(YEAR FROM \1::timestamp)",
            sql_code,
            flags=re.IGNORECASE
        )
        # MONTH(X) -> EXTRACT(MONTH FROM X::timestamp)
        sql_code = re.sub(
            r"MONTH\s*\(\s*([^\)]+)\s*\)",
            r"EXTRACT(MONTH FROM \1::timestamp)",
            sql_code,
            flags=re.IGNORECASE
        )
        
        # TO_TIMESTAMP(X::text, 'YYYY-MM-DD') -> DATE_TRUNC('day', X)
        sql_code = re.sub(r"TO_TIMESTAMP\(([^)]+?)::text,\s*'YYYY-MM-DD'\)", r"DATE_TRUNC('day', \1)", sql_code)
        
        # --- Новый блок: исправляем сравнения числовых полей с пустой строкой ---
        from src.parser import SQLParser
        parser = SQLParser(self.config)
        alias_analyzer = self.alias_analyzer
        import psycopg2
        pg = self.config.PG_CONFIG
        def get_column_type(table, column):
            try:
                with psycopg2.connect(
                    host=pg['host'], port=pg['port'], database=pg['database'], user=pg['user'], password=pg['password']
                ) as conn, conn.cursor() as cur:
                    cur.execute("""
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_name = %s AND column_name = %s
                        LIMIT 1
                    """, (table.lower(), column.lower()))
                    row = cur.fetchone()
                    if row:
                        return row[0]
            except Exception as e:
                print(f"[post_process_sql] Ошибка при получении типа для {table}.{column}: {e}")
            return None

        def fix_empty_string_comparison(match):
            field = match.group(1)
            # Попробуем вытащить alias и column
            if '.' in field:
                alias, column = field.split('.', 1)
                table = alias_analyzer.get_table_by_alias(sql_code, alias)
            else:
                table, column = None, field
            col_type = None
            if table:
                col_type = get_column_type(table, column)
            # Если числовой тип — исправляем
            if col_type and col_type.lower() in [
                'double precision', 'numeric', 'integer', 'float', 'real', 'bigint', 'smallint', 'decimal']:
                return f"{field} IS NULL"
            else:
                return match.group(0)

        sql_code = re.sub(
            r"([\w\.]+)\s*=\s*''(::text)?",
            fix_empty_string_comparison,
            sql_code
        )
        # --- конец нового блока ---

        return sql_code