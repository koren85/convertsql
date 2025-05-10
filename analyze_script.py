#!/usr/bin/env python3
"""
Скрипт для детального анализа SQL-скрипта и его структуры
"""

import os
import sys
import argparse
import json
import re
import sqlparse
from pathlib import Path

# Импортируем наши модули
import config
from src.parser import SQLParser

def analyze_sql_structure(sql_content):
    """
    Анализирует структуру SQL-скрипта
    """
    # Форматируем SQL для лучшего анализа
    formatted_sql = sqlparse.format(sql_content, reindent=True, keyword_case='upper')
    
    # Разбиваем на отдельные запросы
    statements = sqlparse.split(formatted_sql)
    
    # Анализируем каждый запрос
    results = []
    
    for i, statement in enumerate(statements):
        if not statement.strip():
            continue
            
        parsed = sqlparse.parse(statement)[0]
        
        # Определяем тип запроса
        statement_type = parsed.get_type() if hasattr(parsed, 'get_type') else 'UNKNOWN'
        
        # Анализируем токены
        tokens = list(parsed.flatten())
        
        # Ищем таблицы
        tables = []
        for token in tokens:
            if token.ttype is None and isinstance(token, sqlparse.sql.Identifier):
                # Это может быть имя таблицы
                tables.append(token.value)
                
        # Ищем условия WHERE
        where_clauses = []
        where_section = False
        current_where = ""
        
        for token in tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'WHERE':
                where_section = True
                current_where = ""
            elif where_section and token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('GROUP', 'ORDER', 'HAVING'):
                where_section = False
                if current_where.strip():
                    where_clauses.append(current_where.strip())
            elif where_section:
                current_where += str(token)
                
        if where_section and current_where.strip():
            where_clauses.append(current_where.strip())
            
        # Ищем функции MS SQL
        mssql_functions = re.findall(r'\b(ISNULL|GETDATE|CONVERT|DATEADD|DATEDIFF|TOP)\b', statement, re.IGNORECASE)
        mssql_functions = list(set([func.upper() for func in mssql_functions]))
        
        # Ищем типы данных
        data_types = re.findall(r'\b(NVARCHAR|VARCHAR|NCHAR|CHAR|DATETIME|DATE|INT|BIGINT|UNIQUEIDENTIFIER)\b', 
                              statement, re.IGNORECASE)
        data_types = list(set([dtype.upper() for dtype in data_types]))
        
        # Проверяем наличие CTE
        has_cte = 'WITH' in [token.value.upper() for token in tokens if token.ttype is sqlparse.tokens.Keyword]
        
        # Проверяем наличие оконных функций
        has_window_functions = any(re.search(r'\bOVER\s*\(', token.value, re.IGNORECASE) 
                                 for token in tokens if hasattr(token, 'value'))
        
        # Проверяем наличие специфичного синтаксиса MS SQL
        has_square_brackets = '[' in statement and ']' in statement
        
        results.append({
            'statement_number': i + 1,
            'statement_type': statement_type,
            'tables': tables,
            'where_clauses': where_clauses,
            'mssql_functions': mssql_functions,
            'data_types': data_types,
            'has_cte': has_cte,
            'has_window_functions': has_window_functions,
            'has_square_brackets': has_square_brackets,
            'statement': statement
        })
    
    return results

def analyze_params(sql_content):
    """
    Анализирует параметры в SQL-скрипте
    """
    # Ищем параметры в формате {param_name}
    params = re.findall(r'\{([^\}]+)\}', sql_content)
    unique_params = list(set(params))
    
    # Анализируем каждый параметр
    param_analysis = []
    
    for param in unique_params:
        # Считаем количество использований
        count = len(re.findall(r'\{' + re.escape(param) + r'\}', sql_content))
        
        # Находим контекст использования
        contexts = []
        for match in re.finditer(r'(.{0,30})\{' + re.escape(param) + r'\}(.{0,30})', sql_content):
            context = f"{match.group(1)}<<{param}>>{match.group(2)}"
            contexts.append(context)
        
        param_analysis.append({
            'param_name': param,
            'occurrences': count,
            'contexts': contexts,
            'default_value': config.DEFAULT_PARAMS.get(param, 'Не задано')
        })
    
    return param_analysis

def analyze_conversion_issues(sql_content):
    """
    Анализирует возможные проблемы при конвертации
    """
    issues = []
    
    # Проверяем наличие временных таблиц с @
    if re.search(r'@\w+', sql_content):
        issues.append({
            'severity': 'high',
            'issue': 'Временные таблицы с @',
            'description': 'MS SQL использует синтаксис @table_name для переменных таблиц. В PostgreSQL временные таблицы должны быть созданы с использованием CREATE TEMP TABLE.',
            'solution': 'Заменить @table_name на временные таблицы PostgreSQL.'
        })
    
    # Проверяем наличие операторов TOP без ORDER BY
    if re.search(r'SELECT\s+TOP\s+\d+', sql_content, re.IGNORECASE) and not re.search(r'ORDER\s+BY', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'medium',
            'issue': 'TOP без ORDER BY',
            'description': 'Использование TOP без ORDER BY может привести к недетерминированным результатам. PostgreSQL использует LIMIT и рекомендуется указывать порядок сортировки.',
            'solution': 'Добавить ORDER BY перед использованием LIMIT.'
        })
    
    # Проверяем наличие OUTPUT-операторов
    if re.search(r'\bOUTPUT\b', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'high',
            'issue': 'Оператор OUTPUT',
            'description': 'MS SQL использует OUTPUT для возврата данных из операторов INSERT/UPDATE/DELETE. PostgreSQL использует RETURNING.',
            'solution': 'Заменить OUTPUT на RETURNING.'
        })
    
    # Проверяем наличие MERGE-операторов
    if re.search(r'\bMERGE\b', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'high',
            'issue': 'Оператор MERGE',
            'description': 'MS SQL использует MERGE для операций upsert. PostgreSQL имеет другой синтаксис с INSERT ... ON CONFLICT.',
            'solution': 'Заменить MERGE на INSERT ... ON CONFLICT.'
        })
    
    # Проверяем наличие табличных переменных
    if re.search(r'DECLARE\s+@\w+\s+TABLE', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'high',
            'issue': 'Табличные переменные',
            'description': 'MS SQL использует табличные переменные. PostgreSQL использует временные таблицы.',
            'solution': 'Заменить табличные переменные на CREATE TEMP TABLE.'
        })
    
    # Проверяем использование IDENTITY
    if re.search(r'IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'medium',
            'issue': 'IDENTITY',
            'description': 'MS SQL использует IDENTITY(seed, increment) для автоинкрементных полей. PostgreSQL использует SERIAL или GENERATED AS IDENTITY.',
            'solution': 'Заменить IDENTITY на SERIAL или GENERATED AS IDENTITY.'
        })
    
    # Проверяем использование NEWID()
    if re.search(r'\bNEWID\(\)', sql_content, re.IGNORECASE):
        issues.append({
            'severity': 'low',
            'issue': 'Функция NEWID()',
            'description': 'MS SQL использует NEWID() для генерации UUID. PostgreSQL использует gen_random_uuid().',
            'solution': 'Заменить NEWID() на gen_random_uuid().'
        })
    
    return issues

def main():
    """
    Основная функция для запуска анализа
    """
    parser = argparse.ArgumentParser(description='Анализ SQL-скрипта')
    parser.add_argument('script', help='Путь к SQL скрипту для анализа')
    parser.add_argument('--output', help='Сохранить результаты анализа в JSON файл')
    
    args = parser.parse_args()
    
    # Проверяем существование файла
    script_path = Path(args.script)
    if not script_path.exists() or not script_path.is_file():
        print(f"Ошибка: файл {script_path} не существует")
        return 1
    
    # Читаем содержимое скрипта
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    # Анализируем скрипт
    print(f"Анализ скрипта: {script_path}")
    
    # Базовая информация
    sql_parser = SQLParser(config)
    parsed_script = sql_parser.parse_script(script_content)
    
    # Структура SQL
    structure = analyze_sql_structure(script_content)
    
    # Параметры
    params = analyze_params(script_content)
    
    # Возможные проблемы при конвертации
    issues = analyze_conversion_issues(script_content)
    
    # Формируем итоговый отчет
    report = {
        'script_name': script_path.name,
        'script_size': len(script_content),
        'statements_count': len(structure),
        'params_count': len(params),
        'issues_count': len(issues),
        'structure': structure,
        'params': params,
        'issues': issues
    }
    
    # Выводим отчет
    print("\nОТЧЕТ ОБ АНАЛИЗЕ:")
    print(f"Имя скрипта: {report['script_name']}")
    print(f"Размер скрипта: {report['script_size']} байт")
    print(f"Количество операторов: {report['statements_count']}")
    print(f"Количество параметров: {report['params_count']}")
    print(f"Количество потенциальных проблем: {report['issues_count']}")
    
    print("\nТИПЫ ОПЕРАТОРОВ:")
    for stmt in structure:
        print(f"  - {stmt['statement_type']}")
    
    print("\nПАРАМЕТРЫ:")
    for param in params:
        print(f"  - {param['param_name']} (используется {param['occurrences']} раз)")
    
    print("\nПОТЕНЦИАЛЬНЫЕ ПРОБЛЕМЫ:")
    for issue in issues:
        print(f"  - [{issue['severity'].upper()}] {issue['issue']}: {issue['description']}")
    
    # Сохраняем результаты в JSON, если требуется
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        print(f"\nРезультаты анализа сохранены в {output_path}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
