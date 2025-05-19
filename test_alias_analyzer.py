#!/usr/bin/env python3
"""
Скрипт для тестирования работы модуля анализатора алиасов SQL
"""

import os
import sys
from pathlib import Path
import time
import argparse

# Добавляем корневой каталог проекта в путь поиска модулей
sys.path.append(str(Path(__file__).resolve().parent))

# Импортируем наш новый модуль
from src.sql_alias_analyzer import SQLAliasAnalyzer

def test_alias_analyzer(sql_file_path, verbose=False):
    """
    Тестирует работу анализатора алиасов SQL на заданном файле
    
    Args:
        sql_file_path: Путь к файлу SQL для анализа
        verbose: Выводить подробную информацию
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
    
    # Создаем экземпляр анализатора
    analyzer = SQLAliasAnalyzer()
    
    # Тестируем основной метод получения алиасов
    print("\n=== Тестирование метода get_table_aliases ===")
    start_time = time.time()
    table_aliases = analyzer.get_table_aliases(sql_script)
    elapsed_time = time.time() - start_time
    
    print(f"Время выполнения: {elapsed_time:.4f} секунд")
    print(f"Найдено таблиц: {len(table_aliases)}")
    
    # Выводим результаты
    for table, aliases in table_aliases.items():
        if aliases:
            print(f"  {table}: {', '.join(aliases)}")
        else:
            print(f"  {table}: Нет алиаса")
    
    # Тестируем получение таблицы по алиасу
    print("\n=== Тестирование метода get_table_by_alias ===")
    
    if verbose:
        # Собираем все уникальные алиасы
        all_aliases = set()
        for aliases in table_aliases.values():
            all_aliases.update(aliases)
        
        for alias in sorted(all_aliases):
            table = analyzer.get_table_by_alias(sql_script, alias)
            print(f"  Алиас '{alias}' -> Таблица: {table}")
    else:
        # Если нет подробного режима, просто тестируем несколько алиасов из найденных
        test_aliases = []
        for aliases in table_aliases.values():
            if aliases:
                test_aliases.append(aliases[0])
                if len(test_aliases) >= 3:
                    break
        
        for alias in test_aliases:
            table = analyzer.get_table_by_alias(sql_script, alias)
            print(f"  Алиас '{alias}' -> Таблица: {table}")
    
    # Тестируем сопоставление колонок с таблицами
    print("\n=== Тестирование метода get_column_to_table_mapping ===")
    start_time = time.time()
    column_mapping = analyzer.get_column_to_table_mapping(sql_script)
    elapsed_time = time.time() - start_time
    
    print(f"Время выполнения: {elapsed_time:.4f} секунд")
    print(f"Найдено сопоставлений колонок: {len(column_mapping)}")
    
    if verbose or len(column_mapping) < 10:
        for column, table in column_mapping.items():
            print(f"  {column} -> {table}")
    else:
        # Выводим только первые 10 сопоставлений
        print("Первые 10 сопоставлений:")
        for i, (column, table) in enumerate(column_mapping.items()):
            print(f"  {column} -> {table}")
            if i >= 9:
                break
        print(f"  ... и еще {len(column_mapping) - 10} сопоставлений")

def main():
    """
    Основная функция для запуска тестирования
    """
    parser = argparse.ArgumentParser(description='Тестирование анализатора алиасов SQL')
    parser.add_argument('sql_file', help='Путь к файлу SQL для анализа')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    
    args = parser.parse_args()
    
    test_alias_analyzer(args.sql_file, args.verbose)
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 