import re
import sqlparse
import psycopg2
from src.sql_alias_analyzer import SQLAliasAnalyzer

class SQLParser:
    def __init__(self, config):
        self.config = config
        self.alias_analyzer = SQLAliasAnalyzer()
        
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
        Анализирует контекст использования параметра в скрипте для определения его типа
        
        Args:
            script_content: Скрипт, в котором используется параметр
            param_name: Имя параметра без фигурных скобок
            
        Returns:
            str or None: Предполагаемое значение параметра или None, если тип не определён
        """
        # Создаем паттерн для поиска параметра в скрипте
        param_pattern = r'\{' + re.escape(param_name) + r'\}'
        
        # Используем SQLAliasAnalyzer для поиска контекста параметра
        # Ищем паттерны вида alias.column = {param_name}
        column_param_pattern = rf'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*{param_pattern}|{param_pattern}\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
        for match in re.finditer(column_param_pattern, script_content):
            if match.group(1) and match.group(2):  # alias.column = {param}
                alias, column = match.group(1), match.group(2)
            else:  # {param} = alias.column
                alias, column = match.group(3), match.group(4)
            
            # Определяем таблицу по алиасу
            table = self.alias_analyzer.get_table_by_alias(script_content, alias)
            if table:
                print(f"[analyze_param_context] Найден контекст: {param_name} сравнивается с {alias}.{column} (таблица: {table})")
                
                # По имени колонки определяем тип параметра
                if any(id_term in column.lower() for id_term in ['id', 'ouid', 'code', 'status', 'mspholder']):
                    print(f"[analyze_param_context] Колонка {column} похожа на ID, возвращаем 1")
                    return 1
                elif any(date_term in column.lower() for date_term in ['date', 'time', 'reg', 'period']):
                    print(f"[analyze_param_context] Колонка {column} похожа на дату, возвращаем timestamp")
                    return "'2023-01-01'::timestamp"
                elif 'name' in column.lower() or 'text' in column.lower():
                    print(f"[analyze_param_context] Колонка {column} похожа на текст, возвращаем строку")
                    return "'test'"
                
        # Проверка на числовой тип параметра
        number_patterns = [
            rf"{param_pattern}\s*(?:=|!=|<>|>|<|>=|<=)\s*\d+",
            rf"\d+\s*(?:=|!=|<>|>|<|>=|<=)\s*{param_pattern}",
            rf"{param_pattern}\s*IN\s*\(\s*\d+",
            rf"{param_pattern}\s*::\s*(?:integer|bigint|smallint|numeric|decimal|int)"
        ]
        
        for pattern in number_patterns:
            if re.search(pattern, script_content, re.IGNORECASE):
                print(f"[analyze_param_context] Определил параметр {param_name} как число на основе контекста")
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
            
        # Анализируем алиасы таблиц для лучшего понимания контекста параметров
        table_aliases = self.alias_analyzer.get_table_aliases(script_content)
        column_to_table = self.alias_analyzer.get_column_to_table_mapping(script_content)
        
        # Выводим найденные алиасы для отладки
        print(f"[replace_params] Обнаружены следующие алиасы таблиц:")
        for table, aliases in table_aliases.items():
            if aliases:
                print(f"  {table}: {', '.join(aliases)}")
            else:
                print(f"  {table}: Нет алиаса")
                
        # 0. Спецобработка: если есть сравнение двух параметров — оба подставлять как 1
        param_cmp_pattern = r'(\{params\.[^\}]+\})\s*=\s*(\{params\.[^\}]+\})'
        for m in re.finditer(param_cmp_pattern, script_content):
            p1 = m.group(1)[1:-1]  # убираем {}
            p2 = m.group(2)[1:-1]
            params_dict[p1] = 1
            params_dict[p2] = 1
            print(f"[replace_params] Сравнение двух параметров: {p1} = {p2} -> 1 = 1")
        # 0.1. Спецобработка: если есть {PPRCONST.EdDV1y}, {PPRCONST.EdDV2y}, {PPRCONST.EdDV3y} — всегда подставлять 1
        for eddv in ['PPRCONST.EdDV1y', 'PPRCONST.EdDV2y', 'PPRCONST.EdDV3y']:
            if f'{{{eddv}}}' in script_content:
                params_dict[eddv] = 1
                print(f"[replace_params] Спец: {eddv} -> 1")
        # 0.2. Спецобработка: если есть конструкции THEN {PPRCONST.*} — подставлять 1
        for m in re.finditer(r'THEN\s*\{(PPRCONST\.[^\}]+)\}', script_content):
            param = m.group(1)
            params_dict[param] = 1
            print(f"[replace_params] THEN {{{param}}} -> 1")
        # 0.3. Если параметр {PPRCONST.*} участвует в арифметике — подставлять 1
        for m in re.finditer(r'[\+\-\*/]\s*\{(PPRCONST\.[^\}]+)\}|\{(PPRCONST\.[^\}]+)\}\s*[\+\-\*/]', script_content):
            param = m.group(1) or m.group(2)
            params_dict[param] = 1
            print(f"[replace_params] Арифметика с {{{param}}} -> 1")
        # 1. Собираем все параметры
        all_params = set(re.findall(r'\{([^\}]+)\}', script_content))
        
        # 2. Анализируем алиасы таблиц для параметров в выражениях с таблицами
        # Ищем параметры, используемые в сравнениях с колонками таблиц
        # Паттерн: alias.column = {params.value} или {params.value} = alias.column
        col_param_pattern = r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*\{([^\}]+)\}|\{([^\}]+)\}\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
        for m in re.finditer(col_param_pattern, script_content):
            # Определяем параметр и колонку из выражения
            if m.group(3):  # alias.column = {params.value}
                alias, column, param = m.group(1), m.group(2), m.group(3)
            else:  # {params.value} = alias.column
                param, alias, column = m.group(4), m.group(5), m.group(6)
                
            if param in all_params and param not in params_dict:
                # Определяем таблицу по алиасу
                table = self.alias_analyzer.get_table_by_alias(script_content, alias)
                if table:
                    print(f"[replace_params] Параметр {param} используется в сравнении с {alias}.{column} (таблица: {table})")
                    # Подбираем значение в зависимости от имени колонки
                    if any(id_term in column.lower() for id_term in ['id', 'ouid', 'code', 'status']):
                        params_dict[param] = 1
                        print(f"[replace_params] По анализу алиаса и колонки: {param} = 1 (ID-колонка)")
                    elif any(date_term in column.lower() for date_term in ['date', 'time', 'timestamp', 'reg']):
                        params_dict[param] = "'2023-01-01'::timestamp"
                        print(f"[replace_params] По анализу алиаса и колонки: {param} = '2023-01-01'::timestamp (дата)")
                
        # 3. Анализируем контекст использования параметров
        for param in all_params:
            if param not in params_dict:
                val = self.analyze_param_context(script_content, param)
                if val is not None:
                    params_dict[param] = val
                    print(f"[replace_params] На основе анализа контекста: {param} = {val}")
        
        # 4. Применяем жёсткие правила по имени параметра
        for param in all_params:
            if param not in params_dict:
                val = self.get_hardcoded_param_value(param)
                if val is not None:
                    params_dict[param] = val
                    print(f"[replace_params] По шаблону имени: {param} = {val}")
        
        # 5. Для остальных — пробуем через БД
        missing_params = [p for p in all_params if p not in params_dict]
        if missing_params:
            db_param_types = self.guess_param_type_from_db(script_content)
            for p in missing_params:
                if p in db_param_types and db_param_types[p] is not None:
                    params_dict[p] = db_param_types[p]
                    print(f"[replace_params] По БД схеме: {p} = {params_dict[p]}")
        
        # 6. Fallback на строку
        for param in all_params:
            if param not in params_dict:
                params_dict[param] = f"'default_{param}'"
                print(f"[replace_params] Fallback на строку: {param} = {params_dict[param]}")
        
        # 7. Подстановка
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
        # Новый универсальный паттерн: ищет field op {param} и {param} op field
        pattern = r'([a-zA-Z_][\w\.]*)\s*(=|<|>|<=|>=|!=|<>)\s*\{([^\}]+)\}|\{([^\}]+)\}\s*(=|<|>|<=|>=|!=|<>)\s*([a-zA-Z_][\w\.]*)'
        for match in re.finditer(pattern, script_content):
            if match.group(1) and match.group(3):
                field_expr, param_name = match.group(1), match.group(3)
            elif match.group(4) and match.group(6):
                field_expr, param_name = match.group(6), match.group(4)
            else:
                continue
            if param_name in param_types:
                continue
            if '.' in field_expr:
                alias, field = field_expr.split('.', 1)
                table = alias_map.get(alias, alias)  # если алиас не найден — fallback на alias
            else:
                table, field = None, field_expr
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
                elif col_type in ('double precision', 'numeric', 'real', 'float', 'decimal'):
                    param_types[param_name] = 1.0
                elif col_type in ('character varying', 'text', 'varchar', 'citext'):
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
