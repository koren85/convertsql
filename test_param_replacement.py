#!/usr/bin/env python3
"""
Скрипт для тестирования обработки параметров в SQL-скриптах с использованием анализатора алиасов
"""

import sys
import os
import re
from pathlib import Path

# Добавляем путь к родительской директории для импорта
sys.path.append(str(Path(__file__).resolve().parent))

# Импортируем необходимые модули
from src.parser import SQLParser
from src.sql_alias_analyzer import SQLAliasAnalyzer
import config

def test_param_replacement(script_path, params=None):
    """
    Проверяет корректность замены параметров в скрипте с использованием анализатора алиасов
    
    Args:
        script_path: Путь к SQL файлу
        params: Словарь с параметрами для подстановки
    """
    # Если параметры не указаны, используем тестовые значения
    if params is None:
        params = {
            'reciever': 1,
            'startDate': "'2023-01-01'::timestamp",
            'dateRecalc': "'2023-01-01'::timestamp",
            'childId': 1
        }
    
    # Проверяем существование файла
    if not os.path.exists(script_path):
        print(f"Ошибка: Файл {script_path} не найден.")
        return
    
    # Читаем содержимое скрипта
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        print(f"SQL скрипт успешно загружен. Размер: {len(script_content)} байт.")
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return
    
    # Создаем экземпляр парсера
    parser = SQLParser(config)
    
    # Сначала выводим информацию об алиасах
    print("\n=== Анализ алиасов таблиц ===")
    table_aliases = parser.alias_analyzer.get_table_aliases(script_content)
    for table, aliases in table_aliases.items():
        if aliases:
            print(f"  {table}: {', '.join(aliases)}")
        else:
            print(f"  {table}: Нет алиаса")
    
    # Выводим сопоставление алиасов с колонками
    print("\n=== Сопоставление колонок с таблицами ===")
    column_mapping = parser.alias_analyzer.get_column_to_table_mapping(script_content)
    for column, table in column_mapping.items():
        print(f"  {column} -> {table}")
    
    # Находим все параметры в скрипте
    print("\n=== Параметры в скрипте ===")
    all_params = set(re.findall(r'\{([^\}]+)\}', script_content))
    for param in sorted(all_params):
        print(f"  {param}")
    
    # Выполняем замену параметров
    print("\n=== Выполняем замену параметров ===")
    result = parser.replace_params(script_content, params)
    
    # Выводим результат
    print("\n=== Результат после замены параметров ===\n")
    print(result)
    
    # Проверяем наличие незаменённых параметров
    if re.search(r'\{[^\}]+\}', result):
        print("\n⚠️ ВНИМАНИЕ: В результате всё ещё есть незаменённые параметры!")
        params = re.findall(r'\{([^\}]+)\}', result)
        print(f"Незаменённые параметры: {params}")
    else:
        print("\n✅ Все параметры успешно заменены")

def main():
    """
    Основная функция для запуска тестирования
    """
    # Проверяем переданные аргументы
    if len(sys.argv) < 2:
        print("Использование: python test_param_replacement.py <путь_к_файлу_sql>")
        return 1
    
    script_path = sys.argv[1]
    test_param_replacement(script_path)
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 