#!/usr/bin/env python3
"""
Скрипт для проверки конвертации примеров из директории examples
"""

import os
import sys
import argparse
from pathlib import Path
from tabulate import tabulate

# Импортируем наши модули
import config
from src.parser import SQLParser
from src.converter import SQLConverter
from src.postgres_tester import PostgresTester
from src.logger import Logger

def check_example(script_path, verbose=False):
    """
    Проверяет конвертацию одного примера
    """
    script_name = os.path.basename(script_path)
    
    # Создаем объекты для работы со скриптом
    parser = SQLParser(config)
    converter = SQLConverter(config)
    tester = PostgresTester(config)
    
    # Читаем содержимое скрипта
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    try:
        # Парсим скрипт
        parsed_script = parser.parse_script(script_content)
        
        # Конвертируем скрипт
        converted_script = converter.convert(parsed_script)
        
        # Заменяем параметры на значения по умолчанию
        script_with_params = parser.replace_params(converted_script)
        
        # Тестируем в PostgreSQL
        result = tester.test_script(script_with_params)
        
        # Если не удалось с первого раза, пробуем исправить
        if not result['success']:
            converted_script = tester.fix_script(converted_script, result['error'])
            script_with_params = parser.replace_params(converted_script)
            result = tester.test_script(script_with_params)
        
        if verbose:
            if result['success']:
                print(f"✅ {script_name}: Успешно сконвертирован и выполнен")
            else:
                print(f"❌ {script_name}: Ошибка выполнения: {result['error']}")
        
        return {
            'script': script_name,
            'success': result['success'],
            'error': result['error'] if not result['success'] else None,
            'execution_time': result['execution_time'],
            'row_count': result['row_count'],
            'original_size': len(script_content),
            'converted_size': len(converted_script),
            'changes_count': _count_changes(script_content, converted_script)
        }
    except Exception as e:
        if verbose:
            print(f"❌ {script_name}: Ошибка обработки: {str(e)}")
        
        return {
            'script': script_name,
            'success': False,
            'error': str(e),
            'execution_time': None,
            'row_count': None,
            'original_size': len(script_content),
            'converted_size': 0,
            'changes_count': 0
        }

def _count_changes(original, converted):
    """
    Подсчитывает количество изменений между исходным и сконвертированным скриптами
    """
    # Простой подсчет по ключевым словам
    changes = 0
    
    if "[" in original and "\"" in converted:
        # Количество замен квадратных скобок
        changes += original.count("[")
    
    if "TOP" in original and "LIMIT" in converted:
        # Замены TOP на LIMIT
        changes += original.count("TOP")
    
    if "ISNULL" in original and "COALESCE" in converted:
        # Замены ISNULL на COALESCE
        changes += original.count("ISNULL")
    
    if "CONVERT" in original and "CAST" in converted:
        # Замены CONVERT на CAST
        changes += original.count("CONVERT")
    
    if "DATEADD" in original and "INTERVAL" in converted:
        # Замены DATEADD
        changes += original.count("DATEADD")
    
    if "GETDATE()" in original and "CURRENT_TIMESTAMP" in converted:
        # Замены GETDATE()
        changes += original.count("GETDATE()")
    
    # Добавляем изменения типов данных
    for ms_type in ["NVARCHAR", "DATETIME", "UNIQUEIDENTIFIER"]:
        if ms_type in original:
            changes += original.count(ms_type)
    
    return changes

def main():
    """
    Основная функция для проверки примеров
    """
    parser = argparse.ArgumentParser(description='Проверка конвертации примеров SQL скриптов')
    parser.add_argument('--dir', default='scripts/examples', help='Директория с примерами')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    parser.add_argument('--output', help='Сохранить сконвертированные скрипты в указанную директорию')
    
    args = parser.parse_args()
    
    # Проверяем существование директории
    examples_dir = Path(args.dir)
    if not examples_dir.exists() or not examples_dir.is_dir():
        print(f"Ошибка: директория {examples_dir} не существует")
        return 1
    
    # Находим все SQL-скрипты в директории
    examples = list(examples_dir.glob('*.sql'))
    if not examples:
        print(f"В директории {examples_dir} не найдено SQL-скриптов")
        return 1
    
    print(f"Найдено {len(examples)} примеров для проверки")
    
    # Проверяем Docker
    tester = PostgresTester(config)
    try:
        tester.ensure_docker_running()
        print("Docker контейнер с PostgreSQL запущен")
    except Exception as e:
        print(f"Ошибка при проверке Docker контейнера: {str(e)}")
        print("Запустите 'docker-compose up -d' перед использованием скрипта")
        return 1
    
    # Проверяем все примеры
    results = []
    
    for example in examples:
        print(f"Проверка {example.name}...")
        result = check_example(example, args.verbose)
        results.append(result)
        
        # Сохраняем сконвертированный скрипт, если указана директория
        if args.output and result['success']:
            output_dir = Path(args.output)
            output_dir.mkdir(exist_ok=True)
            
            # Получаем сконвертированную версию
            parser = SQLParser(config)
            converter = SQLConverter(config)
            
            with open(example, 'r', encoding='utf-8') as f:
                script_content = f.read()
            
            parsed_script = parser.parse_script(script_content)
            converted_script = converter.convert(parsed_script)
            
            # Сохраняем результат
            output_path = output_dir / example.name
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(converted_script)
    
    # Выводим результаты в виде таблицы
    headers = ['Скрипт', 'Статус', 'Время (сек)', 'Строк', 'Размер (ориг.)', 'Размер (конв.)', 'Изменения']
    table_data = []
    
    for r in results:
        status = "✅ Успешно" if r['success'] else f"❌ Ошибка: {r['error']}"
        time_str = f"{r['execution_time']:.4f}" if r['execution_time'] is not None else "N/A"
        rows_str = str(r['row_count']) if r['row_count'] is not None else "N/A"
        
        table_data.append([
            r['script'],
            status,
            time_str,
            rows_str,
            r['original_size'],
            r['converted_size'],
            r['changes_count']
        ])
    
    print("\n" + tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # Итоговая статистика
    success_count = sum(1 for r in results if r['success'])
    print(f"\nИтоги: {success_count} из {len(results)} скриптов успешно сконвертированы и выполнены")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
