import re
import sqlparse
import psycopg2

class SQLParser:
    def __init__(self, config):
        self.config = config
        
    def parse_script(self, script_content):
        """
        Парсит SQL скрипт, выделяя параметры и структуру запроса
        """
        # Найти все параметры в формате {param_name}
        params = re.findall(r'\{([^\}]+)\}', script_content)
        unique_params = list(set(params))
        
        # Разбить скрипт на отдельные запросы
        statements = sqlparse.split(script_content)
        
        return {
            'params': unique_params,
            'statements': statements,
            'original': script_content
        }
    
    def get_hardcoded_param_value(self, param_name):
        name = param_name.lower()
        
        # Специальная обработка для DOC.* параметров 
        if name.startswith('doc.'):
            # Для параметров DOC.* (которые обычно используются в IN) возвращаем (1,1)
            return "(1,1)"
            
        if 'id' in name or 'ouid' in name or 'a_code' in name or 'status' in name or 'mspholder' in name:
            return 1
        if 'date' in name or 'time' in name or 'timestamp' in name or 'reg' in name or 'period' in name:
            return "'2023-01-01'::timestamp"
        if 'code' in name:
            return "'testcode'"
        # ...добавь свои правила по необходимости
        return None

    def analyze_param_context(self, script_content, param_name):
        """
        Анализирует контекст параметра в SQL запросе для определения его типа
        
        Args:
            script_content: Содержимое SQL скрипта
            param_name: Имя параметра без фигурных скобок
            
        Returns:
            str: Значение по умолчанию для параметра или None, если тип не определен
        """
        # Проверка специальных параметров DOC.* на основе имени
        if param_name.lower().startswith('doc.'):
            print(f"[analyze_param_context] Определил параметр {param_name} как DOC.* для IN-конструкции")
            return "(1,1)"
            
        param_pattern = r'\{' + re.escape(param_name) + r'\}'
        
        # Проверка контекста использования - особенно для IN-конструкций
        in_pattern = rf"\bIN\s+{param_pattern}"
        if re.search(in_pattern, script_content, re.IGNORECASE):
            print(f"[analyze_param_context] Определил параметр {param_name} для IN-конструкции")
            # Если параметр используется в конструкции IN, подставляем список из одного значения
            return "(1)"
        
        # Шаблоны, указывающие на то, что параметр является датой
        date_patterns = [
            rf"{param_pattern}\s*-\s*INTERVAL",
            rf"{param_pattern}\s*\+\s*INTERVAL",
            rf"BETWEEN\s+\S+\s+AND\s+{param_pattern}",
            rf"BETWEEN\s+{param_pattern}\s+AND",
            rf"DATE_PART\s*\([^,]+,\s*{param_pattern}",
            rf"{param_pattern}\s*::\s*(?:timestamp|date)",
            rf"TO_DATE\s*\(\s*{param_pattern}",
            rf"TO_TIMESTAMP\s*\(\s*{param_pattern}",
            rf"DATE_TRUNC\s*\([^,]+,\s*{param_pattern}",
            rf"EXTRACT\s*\([^FROM]+FROM\s+{param_pattern}"
        ]
        
        for pattern in date_patterns:
            if re.search(pattern, script_content, re.IGNORECASE):
                print(f"[analyze_param_context] Определил параметр {param_name} как дату на основе контекста")
                return "'2023-01-01'::timestamp"
        
        # Проверка на целочисленный тип по контексту
        int_patterns = [
            rf"{param_pattern}\s*(?:=|!=|<>|>|<|>=|<=)\s*\d+(?!\.\d+)(?!::)",
            rf"\d+(?!\.\d+)(?!::)\s*(?:=|!=|<>|>|<|>=|<=)\s*{param_pattern}",
            rf"{param_pattern}\s*IN\s*\(\s*\d+(?!\.\d+)",
            rf"LIMIT\s+{param_pattern}",
            rf"OFFSET\s+{param_pattern}"
        ]
        
        for pattern in int_patterns:
            if re.search(pattern, script_content, re.IGNORECASE):
                print(f"[analyze_param_context] Определил параметр {param_name} как целое число на основе контекста")
                return 1
        
        # Проверка на строковый тип по контексту
        string_patterns = [
            rf"{param_pattern}\s*(?:=|!=|<>|LIKE)\s*'[^']+'",
            rf"'[^']+'\s*(?:=|!=|<>|LIKE)\s*{param_pattern}",
            rf"{param_pattern}\s*IN\s*\(\s*'[^']+'",
            rf"{param_pattern}\s*::\s*(?:varchar|text|citext)"
        ]
        
        for pattern in string_patterns:
            if re.search(pattern, script_content, re.IGNORECASE):
                print(f"[analyze_param_context] Определил параметр {param_name} как строку на основе контекста")
                return "'test'"
        
        return None

    def replace_params(self, script_content, params_dict=None):
        """
        Заменяет параметры в скрипте на значения по умолчанию или из словаря
        """
        if params_dict is None:
            params_dict = {}
        # 1. Собираем все параметры
        all_params = set(re.findall(r'\{([^\}]+)\}', script_content))
        
        # 2. Анализируем контекст использования параметров
        for param in all_params:
            if param not in params_dict:
                val = self.analyze_param_context(script_content, param)
                if val is not None:
                    params_dict[param] = val
                    print(f"[replace_params] На основе анализа контекста: {param} = {val}")
        
        # 3. Применяем жёсткие правила по имени параметра
        for param in all_params:
            if param not in params_dict:
                val = self.get_hardcoded_param_value(param)
                if val is not None:
                    params_dict[param] = val
                    print(f"[replace_params] По шаблону имени: {param} = {val}")
        
        # 4. Для остальных — пробуем через БД
        missing_params = [p for p in all_params if p not in params_dict]
        if missing_params:
            db_param_types = self.guess_param_type_from_db(script_content)
            for p in missing_params:
                if p in db_param_types and db_param_types[p] is not None:
                    params_dict[p] = db_param_types[p]
                    print(f"[replace_params] По БД схеме: {p} = {params_dict[p]}")
        
        # 5. Fallback на строку
        for param in all_params:
            if param not in params_dict:
                params_dict[param] = f"'default_{param}'"
                print(f"[replace_params] Fallback на строку: {param} = {params_dict[param]}")
        
        # 6. Подстановка
        for param in all_params:
            script_content = script_content.replace(f'{{{param}}}', str(params_dict[param]))
        
        return script_content

    def guess_param_type_from_db(self, script_content):
        """
        Возвращает словарь {param_name: value} для подстановки, основываясь на типах полей в БД.
        Если не удалось определить тип — value = None.
        Требует self.config.DB_CONN (psycopg2 connection) или self.config.PG_CONFIG для создания соединения.
        Теперь учитывает alias -> table_name для FROM/JOIN.
        """
        import sqlparse
        param_types = {}
        pattern = r'([a-zA-Z_][\w\.]*)\s*=\s*\{([^\}]+)\}'
        conn = getattr(self.config, 'DB_CONN', None)
        close_conn = False
        print("[guess_param_type_from_db] Пытаюсь получить соединение с БД...")
        if conn is None and hasattr(self.config, 'PG_CONFIG'):
            try:
                import psycopg2
                pg = self.config.PG_CONFIG
                conn = psycopg2.connect(
                    host=pg['host'], port=pg['port'], database=pg['database'], user=pg['user'], password=pg['password']
                )
                close_conn = True
                print("[guess_param_type_from_db] Соединение с БД установлено.")
            except Exception as e:
                print(f"[guess_param_type_from_db] Не удалось создать соединение с БД: {e}")
                conn = None
        if conn is None:
            print("[guess_param_type_from_db] Нет соединения с БД, возвращаю пустой словарь.")
            return param_types
        # --- Новый блок: строим alias_map ---
        def extract_alias_map(sql):
            from sqlparse.sql import Identifier, IdentifierList
            from sqlparse.tokens import Keyword
            alias_map = {}
            parsed = sqlparse.parse(sql)
            for stmt in parsed:
                from_seen = False
                def process_token(token):
                    nonlocal from_seen
                    if from_seen:
                        if isinstance(token, IdentifierList):
                            for identifier in token.get_identifiers():
                                real = identifier.get_real_name()
                                alias = identifier.get_alias()
                                if alias:
                                    alias_map[alias] = real
                        elif isinstance(token, Identifier):
                            real = token.get_real_name()
                            alias = token.get_alias()
                            if alias:
                                alias_map[alias] = real
                        elif hasattr(token, 'is_group') and token.is_group:
                            for t in token.tokens:
                                process_token(t)
                        # Не break — ищем все FROM/JOIN
                    elif token.ttype is Keyword and token.value.upper() in ('FROM', 'JOIN'):
                        from_seen = True
                for token in stmt.tokens:
                    process_token(token)
            return alias_map
        try:
            alias_map = extract_alias_map(script_content)
        except Exception as e:
            print(f"[guess_param_type_from_db] Ошибка при парсинге алиасов: {e}")
            alias_map = {}
        print(f"[guess_param_type_from_db] alias_map: {alias_map}")
        # --- Конец блока alias_map ---
        for match in re.finditer(pattern, script_content):
            field_expr, param_name = match.groups()
            if '.' in field_expr:
                alias, field = field_expr.split('.', 1)
                table = alias_map.get(alias, alias)  # если алиас не найден — fallback на alias
            else:
                table, field = None, field_expr
            if param_name in param_types:
                continue
            col_type = None
            if table:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT data_type
                            FROM information_schema.columns
                            WHERE table_name = %s AND column_name = %s
                            LIMIT 1
                        """, (table.lower(), field.lower()))
                        row = cur.fetchone()
                        if row:
                            col_type = row[0]
                            print(f"[guess_param_type_from_db] {table}.{field} (param: {param_name}) — тип: {col_type}")
                except Exception as e:
                    print(f"[guess_param_type_from_db] Не удалось получить тип для {table}.{field}: {e}")
            if col_type is not None:
                if col_type in ('integer', 'bigint', 'smallint'):
                    param_types[param_name] = 1
                elif col_type in ('character varying', 'text', 'varchar'):
                    param_types[param_name] = "'test'"
                elif col_type in ('date',):
                    param_types[param_name] = "'2023-01-01'::date"
                elif col_type in ('timestamp without time zone', 'timestamp with time zone'):
                    param_types[param_name] = "'2023-01-01 00:00:00'::timestamp"
                else:
                    param_types[param_name] = "'default'"
                print(f"[guess_param_type_from_db] param_types[{param_name}] = {param_types[param_name]}")
            else:
                param_types[param_name] = None
                print(f"[guess_param_type_from_db] Не удалось определить тип для {param_name}, value=None")
        if close_conn:
            conn.close()
            print("[guess_param_type_from_db] Соединение с БД закрыто.")
        print(f"[guess_param_type_from_db] Итоговый param_types: {param_types}")
        return param_types
