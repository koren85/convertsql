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
        if 'id' in name or 'ouid' in name or 'a_code' in name or 'status' in name or 'mspholder' in name:
            return 1
        if 'date' in name or 'time' in name or 'timestamp' in name:
            return "'2023-01-01'::timestamp"
        if 'code' in name:
            return "'testcode'"
        # ...добавь свои правила по необходимости
        return None

    def replace_params(self, script_content, params_dict=None):
        """
        Заменяет параметры в скрипте на значения по умолчанию или из словаря
        """
        if params_dict is None:
            params_dict = {}
        # 1. Собираем все параметры
        all_params = set(re.findall(r'\{([^\}]+)\}', script_content))
        # 2. Получаем значения по жёсткой логике
        for param in all_params:
            val = self.get_hardcoded_param_value(param)
            if val is not None:
                params_dict[param] = val
        # 3. Для остальных — пробуем через БД
        missing_params = [p for p in all_params if p not in params_dict]
        if missing_params:
            db_param_types = self.guess_param_type_from_db(script_content)
            for p in missing_params:
                if p in db_param_types and db_param_types[p] is not None:
                    params_dict[p] = db_param_types[p]
        # 4. Fallback на строку
        for param in all_params:
            if param not in params_dict:
                params_dict[param] = f"'default_{param}'"
        # 5. Подстановка
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
