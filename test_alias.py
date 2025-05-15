import os
import time
import sqlparse
import re
from collections import defaultdict

# Попытаемся импортировать дополнительные библиотеки, но продолжим работу даже если их нет
AVAILABLE_LIBRARIES = {
    'sqlglot': False,
    'parsimonious': False,
    'pglast': False
}

try:
    import sqlglot
    from sqlglot import parse as sqlglot_parse
    AVAILABLE_LIBRARIES['sqlglot'] = True
except ImportError:
    print("Библиотека sqlglot не установлена. Этот метод будет пропущен.")

try:
    from parsimonious.grammar import Grammar
    from parsimonious.nodes import NodeVisitor
    AVAILABLE_LIBRARIES['parsimonious'] = True
except ImportError:
    print("Библиотека parsimonious не установлена. Этот метод будет пропущен.")

try:
    import pglast
    from pglast import parse_sql
    AVAILABLE_LIBRARIES['pglast'] = True
except ImportError:
    print("Библиотека pglast не установлена. Этот метод будет пропущен.")


#---------------------------------
# Метод 1: Использование sqlparse
#---------------------------------
def extract_table_aliases_sqlparse(sql_script):
    """
    Извлечение алиасов таблиц с помощью sqlparse и регулярных выражений
    
    Args:
        sql_script (str): SQL скрипт для анализа
        
    Returns:
        dict: Словарь соответствия имен таблиц их алиасам
    """
    # Разбор SQL скрипта
    parsed = sqlparse.parse(sql_script)
    
    # Словарь для хранения таблица -> алиасы
    table_aliases = defaultdict(list)
    
    # Регулярные выражения для различных шаблонов алиасов
    # Шаблон 1: "имя_таблицы AS алиас" или "имя_таблицы алиас"
    alias_pattern1 = re.compile(r'(\w+)(?:\s+(?:AS\s+)?|\s+AS\s+)(\w+)(?=\s+|$|\))')
    
    # Шаблон 2: Таблицы с указанием схемы: "схема.имя_таблицы AS алиас" или "схема.имя_таблицы алиас"
    alias_pattern2 = re.compile(r'(\w+\.\w+)(?:\s+(?:AS\s+)?|\s+AS\s+)(\w+)(?=\s+|$|\))')
    
    # Обработка каждого оператора в скрипте
    for statement in parsed:
        # Поиск всех предложений FROM и JOIN
        from_join_tokens = []
        current_tokens = []
        in_from_or_join = False
        
        for token in statement.flatten():
            token_type = token.ttype
            token_value = str(token).upper()
            
            # Проверка, входим ли мы в предложение FROM или JOIN
            if token_type in sqlparse.tokens.Keyword and token_value in ('FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN'):
                in_from_or_join = True
                if current_tokens:
                    from_join_tokens.append(' '.join(current_tokens))
                    current_tokens = []
            
            # Если мы в предложении FROM или JOIN, собираем токены
            if in_from_or_join:
                current_tokens.append(str(token))
                
                # Условие выхода из предложения FROM/JOIN
                if token_value in ('WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'INTERSECT', 'EXCEPT', 'LIMIT'):
                    in_from_or_join = False
                    from_join_tokens.append(' '.join(current_tokens[:-1]))  # Исключаем текущий токен
                    current_tokens = []
        
        # Добавляем оставшиеся токены
        if current_tokens:
            from_join_tokens.append(' '.join(current_tokens))
        
        # Извлекаем алиасы таблиц из предложений FROM и JOIN
        sql_text = str(statement)
        
        # Попытка найти соответствия, используя шаблон 1
        for match in alias_pattern1.finditer(sql_text):
            table, alias = match.groups()
            # Проверка, что это действительно ссылка на таблицу, а не на столбец или функцию
            context_before = sql_text[max(0, match.start() - 20):match.start()]
            if 'FROM' in context_before.upper() or 'JOIN' in context_before.upper():
                table_aliases[table].append(alias)
        
        # Попытка найти соответствия, используя шаблон 2 (таблицы с указанием схемы)
        for match in alias_pattern2.finditer(sql_text):
            table, alias = match.groups()
            # Проверка, что это действительно ссылка на таблицу, а не на столбец или функцию
            context_before = sql_text[max(0, match.start() - 20):match.start()]
            if 'FROM' in context_before.upper() or 'JOIN' in context_before.upper():
                table_aliases[table].append(alias)
                
        # Ручной разбор для более сложных случаев
        for clause in from_join_tokens:
            # Удаляем ключевые слова SQL
            clause = re.sub(r'\b(FROM|JOIN|INNER|LEFT|RIGHT|FULL|CROSS|ON|AND|OR)\b', ' ', clause, flags=re.IGNORECASE)
            
            # Разделяем по запятым (для нескольких таблиц в предложении FROM)
            tables = [t.strip() for t in clause.split(',') if t.strip()]
            
            for table_ref in tables:
                # Пропускаем пустые или недопустимые ссылки
                if not table_ref or '=' in table_ref:
                    continue
                    
                # Обрабатываем подзапросы с алиасами
                if '(' in table_ref and ')' in table_ref:
                    subquery_match = re.search(r'\)\s+(?:AS\s+)?(\w+)', table_ref)
                    if subquery_match:
                        alias = subquery_match.group(1)
                        table_aliases['SUBQUERY'].append(alias)
                    continue
                
                # Обрабатываем явные и неявные алиасы
                parts = table_ref.split()
                if len(parts) >= 2:
                    if parts[1].upper() == 'AS' and len(parts) >= 3:
                        table, alias = parts[0], parts[2]
                    else:
                        table, alias = parts[0], parts[1]
                    table_aliases[table].append(alias)
    
    # Находим все таблицы без алиасов
    for statement in parsed:
        sql_text = str(statement)
        
        # Ищем все ссылки на таблицы в предложениях FROM и JOIN
        from_pattern = re.compile(r'FROM\s+(\w+|\w+\.\w+)(?:\s+|$|\))', re.IGNORECASE)
        join_pattern = re.compile(r'JOIN\s+(\w+|\w+\.\w+)(?:\s+|$|\))', re.IGNORECASE)
        
        # Добавляем таблицы без алиасов
        for pattern in [from_pattern, join_pattern]:
            for match in pattern.finditer(sql_text):
                table = match.group(1)
                if table not in table_aliases and not any(a in table.upper() for a in ['SELECT', 'WHERE', 'GROUP', 'ORDER']):
                    table_aliases[table] = []
    
    return dict(table_aliases)


#---------------------------------
# Метод 2: Использование sqlglot
#---------------------------------
def extract_aliases_with_sqlglot(sql_script):
    """
    Извлечение алиасов таблиц с помощью библиотеки sqlglot
    
    Args:
        sql_script (str): SQL скрипт для анализа
        
    Returns:
        dict: Словарь соответствия имен таблиц их алиасам
    """
    if not AVAILABLE_LIBRARIES['sqlglot']:
        return {"Error": "Библиотека sqlglot не установлена"}
    
    try:
        # Разбор SQL скрипта
        parsed = sqlglot_parse(sql_script)
        
        # Словарь для хранения результатов
        table_aliases = defaultdict(list)
        
        # Извлечение всех ссылок на таблицы с их алиасами
        for statement in parsed:
            tables = statement.find_all(sqlglot.exp.Table)
            
            for table in tables:
                table_name = table.name
                alias = table.alias
                
                if alias and alias not in table_aliases[table_name]:
                    table_aliases[table_name].append(alias)
        
        return dict(table_aliases)
    except Exception as e:
        return {"Error": f"Ошибка разбора SQL с помощью sqlglot: {e}"}


#---------------------------------
# Метод 3: Использование parsimonious
#---------------------------------
def extract_aliases_with_parsimonious(sql_script):
    """
    Извлечение алиасов таблиц с помощью библиотеки parsimonious
    
    Args:
        sql_script (str): SQL скрипт для анализа
        
    Returns:
        dict: Словарь соответствия имен таблиц их алиасам
    """
    if not AVAILABLE_LIBRARIES['parsimonious']:
        return {"Error": "Библиотека parsimonious не установлена"}
    
    try:
        # Определяем упрощенную грамматику для ссылок на таблицы и алиасов SQL
        sql_grammar = Grammar(
            r"""
            statement = (table_ref / word / whitespace / punctuation)*
            table_ref = (from_keyword / join_keyword) whitespace+ (schema_qualified_table / simple_table) (whitespace+ "AS" whitespace+ alias / whitespace+ alias)?
            from_keyword = "FROM" / "from"
            join_keyword = "JOIN" / "join" / "INNER JOIN" / "inner join" / "LEFT JOIN" / "left join" / "RIGHT JOIN" / "right join" / "FULL JOIN" / "full join"
            schema_qualified_table = identifier "." identifier
            simple_table = identifier
            alias = identifier
            identifier = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
            word = ~r"[a-zA-Z0-9_]+"
            whitespace = ~r"\s+"
            punctuation = ~r"[^\w\s]"
            """
        )

        class AliasVisitor(NodeVisitor):
            def __init__(self):
                self.table_aliases = defaultdict(list)
                super().__init__()
            
            def visit_table_ref(self, node, visited_children):
                # Извлекаем информацию о таблице и алиасе
                _, _, table_node, alias_part = visited_children
                
                # Определяем имя таблицы
                if len(table_node) > 1:  # schema_qualified_table
                    schema, _, table = table_node[0]
                    table_name = f"{schema}.{table}"
                else:  # simple_table
                    table_name = table_node[0]
                
                # Извлекаем алиас, если он есть
                if alias_part and len(alias_part[0]) > 0:
                    if len(alias_part[0]) > 3:  # С ключевым словом AS
                        alias = alias_part[0][3]
                    else:  # Без ключевого слова AS
                        alias = alias_part[0][1]
                    
                    if alias not in self.table_aliases[table_name]:
                        self.table_aliases[table_name].append(alias)
                
                return node.text
            
            def generic_visit(self, node, visited_children):
                return visited_children or node.text
        
        # Разбор SQL и извлечение алиасов
        visitor = AliasVisitor()
        tree = sql_grammar.parse(sql_script)
        visitor.visit(tree)
        
        return dict(visitor.table_aliases)
    except Exception as e:
        return {"Error": f"Ошибка разбора SQL с помощью parsimonious: {e}"}


#---------------------------------
# Метод 4: Использование pglast
#---------------------------------
def extract_postgres_aliases(sql_script):
    """
    Извлечение алиасов таблиц с помощью библиотеки pglast (специфичной для PostgreSQL)
    
    Args:
        sql_script (str): SQL скрипт для анализа
        
    Returns:
        dict: Словарь соответствия имен таблиц их алиасам
    """
    if not AVAILABLE_LIBRARIES['pglast']:
        return {"Error": "Библиотека pglast не установлена"}
    
    try:
        # Разбор SQL скрипта
        parsed = parse_sql(sql_script)
        
        # Обработка дерева разбора
        tree = pglast.Node(parsed)
        
        # Словарь для хранения результатов
        table_aliases = defaultdict(list)
        
        # Извлечение алиасов из узлов RangeVar
        for node in tree.traverse():
            if hasattr(node, 'node_tag') and node.node_tag == 'RangeVar':
                table_name = node.relname
                alias = node.alias.aliasname if hasattr(node, 'alias') and node.alias else None
                
                if alias and alias not in table_aliases[table_name]:
                    table_aliases[table_name].append(alias)
        
        return dict(table_aliases)
    except Exception as e:
        return {"Error": f"Ошибка разбора SQL с помощью pglast: {e}"}


#---------------------------------
# Функция для запуска всех методов и сравнения результатов
#---------------------------------
def test_all_methods(sql_file_path):
    """
    Тестирование всех методов извлечения алиасов таблиц
    
    Args:
        sql_file_path (str): Путь к файлу SQL для анализа
    """
    # Проверяем, существует ли файл
    if not os.path.exists(sql_file_path):
        print(f"Ошибка: Файл {sql_file_path} не найден.")
        return
    
    # Загружаем SQL скрипт из файла
    print(f"Загрузка SQL из файла: {sql_file_path}")
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        print(f"SQL скрипт успешно загружен. Размер: {len(sql_script)} байт.")
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return
    
    # Тестирование всех методов и измерение времени выполнения
    methods = [
        ("sqlparse (Метод 1)", extract_table_aliases_sqlparse),
        ("sqlglot (Метод 2)", extract_aliases_with_sqlglot),
        ("parsimonious (Метод 3)", extract_aliases_with_parsimonious),
        ("pglast (Метод 4)", extract_postgres_aliases)
    ]
    
    results = {}
    for name, method in methods:
        print(f"\nТестирование метода: {name}")
        
        # Пропускаем методы, для которых нет библиотек
        if name.startswith(("sqlglot", "parsimonious", "pglast")):
            method_name = name.split()[0]
            if not AVAILABLE_LIBRARIES[method_name]:
                print(f"  Пропуск: библиотека {method_name} не установлена.")
                continue
        
        try:
            # Измеряем время выполнения
            start_time = time.time()
            aliases = method(sql_script)
            elapsed_time = time.time() - start_time
            
            results[name] = {
                "aliases": aliases,
                "time": elapsed_time
            }
            
            print(f"  Время выполнения: {elapsed_time:.4f} секунд")
            print(f"  Найдено таблиц: {len(aliases)}")
            
            # Вывод найденных алиасов
            for table, table_aliases in aliases.items():
                if table_aliases:
                    print(f"    {table}: {', '.join(table_aliases)}")
                else:
                    print(f"    {table}: Нет алиаса")
                
        except Exception as e:
            print(f"  Ошибка при выполнении метода: {e}")
    
    # Сравнение результатов разных методов
    if len(results) > 1:
        print("\n=== Сравнение результатов разных методов ===")
        
        # Находим все уникальные таблицы среди всех методов
        all_tables = set()
        for result in results.values():
            all_tables.update(result["aliases"].keys())
        
        # Сравниваем результаты для каждой таблицы
        for table in sorted(all_tables):
            print(f"\nТаблица: {table}")
            
            for name, result in results.items():
                aliases = result["aliases"].get(table, [])
                if aliases:
                    print(f"  {name}: {', '.join(aliases)}")
                else:
                    if table in result["aliases"]:
                        print(f"  {name}: Нет алиаса")
                    else:
                        print(f"  {name}: Таблица не найдена")
    
    # Рекомендации
    print("\n=== Рекомендации ===")
    if "sqlparse (Метод 1)" in results:
        print("Метод 1 (sqlparse): Хорошо работает для большинства случаев, не требует дополнительных библиотек.")
    if "sqlglot (Метод 2)" in results and "Error" not in results["sqlglot (Метод 2)"]["aliases"]:
        print("Метод 2 (sqlglot): Более точен для сложных запросов и поддерживает разные диалекты SQL.")
    if "pglast (Метод 4)" in results and "Error" not in results["pglast (Метод 4)"]["aliases"]:
        print("Метод 4 (pglast): Лучший выбор для PostgreSQL SQL, но может не работать с другими диалектами.")
    
    fastest_method = min(results.items(), key=lambda x: x[1]["time"]) if results else None
    if fastest_method:
        print(f"Самый быстрый метод: {fastest_method[0]} ({fastest_method[1]['time']:.4f} сек)")
    
    most_tables = max(results.items(), key=lambda x: len(x[1]["aliases"])) if results else None
    if most_tables:
        print(f"Метод, нашедший больше всего таблиц: {most_tables[0]} ({len(most_tables[1]['aliases'])} таблиц)")


#---------------------------------
# Запуск тестов
#---------------------------------
if __name__ == "__main__":
    # Путь к файлу SQL по умолчанию
    default_file = "paste.txt"
    
    # Проверяем наличие файла SQL
    if os.path.exists(default_file):
        test_all_methods(default_file)
    else:
        # Если файл не найден, спрашиваем у пользователя
        sql_file = input(f"Файл {default_file} не найден. Введите путь к файлу SQL: ")
        test_all_methods(sql_file)