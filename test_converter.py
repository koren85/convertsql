#!/usr/bin/env python3
"""
Скрипт для тестирования конвертера SQL
"""

import os
import sys
from pathlib import Path
import argparse
import time
import re

# Импортируем наши модули
import config
from src.parser import SQLParser
from src.converter import SQLConverter
from src.postgres_tester import PostgresTester
from src.logger import Logger

def display_script_comparison(original, converted):
    """Выводит сравнение оригинального и сконвертированного скриптов"""
    print("\n" + "="*80)
    print("ОРИГИНАЛЬНЫЙ СКРИПТ:")
    print("="*80)
    print(original)
    
    print("\n" + "="*80)
    print("СКОНВЕРТИРОВАННЫЙ СКРИПТ:")
    print("="*80)
    print(converted)
    
    print("\n" + "="*80)
    print("ОСНОВНЫЕ ИЗМЕНЕНИЯ:")
    print("="*80)
    
    # Выводим ключевые изменения
    changes = []
    if "[" in original and "\"" in converted:
        changes.append("- Квадратные скобки [] заменены на двойные кавычки \"\"")
    
    if "TOP" in original and "LIMIT" in converted:
        changes.append("- Конструкция TOP заменена на LIMIT")
    
    if "ISNULL" in original and "COALESCE" in converted:
        changes.append("- Функция ISNULL заменена на COALESCE")
    
    if "CONVERT" in original and "CAST" in converted:
        changes.append("- Функция CONVERT заменена на CAST")
    
    if "DATEADD" in original and "INTERVAL" in converted:
        changes.append("- Функция DATEADD заменена на оператор INTERVAL")
    
    if "GETDATE()" in original and "CURRENT_TIMESTAMP" in converted:
        changes.append("- Функция GETDATE() заменена на CURRENT_TIMESTAMP")
    
    if "NVARCHAR" in original and "VARCHAR" in converted:
        changes.append("- Тип данных NVARCHAR заменен на VARCHAR")
    
    if len(changes) == 0:
        changes.append("- Нет значительных изменений")
    
    for change in changes:
        print(change)

def test_script(script_path):
    """
    Тестирует конвертацию одного скрипта и показывает результат
    """
    print(f"\nТестирование скрипта: {script_path}")
    
    # Создаем объекты для работы со скриптом
    parser = SQLParser(config)
    converter = SQLConverter(config)
    tester = PostgresTester(config)
    logger = Logger(config)
    
    # Читаем содержимое скрипта
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    print("1. Парсинг скрипта...")
    parsed_script = parser.parse_script(script_content)
    print(f"   Найдено параметров: {len(parsed_script['params'])}")
    if parsed_script['params']:
        print(f"   Параметры: {', '.join(parsed_script['params'])}")
    
    print("\n2. Конвертация скрипта...")
    start_time = time.time()
    converted_script = converter.convert(parsed_script)
    conversion_time = time.time() - start_time
    print(f"   Время конвертации: {conversion_time:.4f} сек")
    
    # Ручная коррекция для CONVERT/CAST
    converted_script = re.sub(r'CAST\(([^,]+),\s*([^,\)]+)(?:\s*,\s*[^\)]+)?\s*\)', 
                             r'CAST(\2 AS \1)', converted_script, flags=re.IGNORECASE)
    
    print("\n3. Замена параметров...")
    script_with_params = parser.replace_params(converted_script)
    
    print("\n4. Тестирование в PostgreSQL...")
    try:
        # tester.ensure_docker_running()
        print("   Используем существующее подключение к PostgreSQL")
        
        start_time = time.time()
        # Используем validate_syntax вместо test_script для проверки синтаксиса
        test_result = tester.validate_syntax(script_with_params)
        test_time = time.time() - start_time
        
        if test_result['success']:
            print(f"   ✅ Синтаксис SQL корректен. Проверка прошла за {test_time:.4f} сек")
        else:
            print(f"   ❌ Ошибка синтаксиса SQL: {test_result['error']}")
            print("   Пытаемся исправить ошибки...")
            
            fixed_script = tester.fix_script(converted_script, test_result['error'])
            script_with_params = parser.replace_params(fixed_script)
            
            start_time = time.time()
            fixed_result = tester.validate_syntax(script_with_params)
            fix_time = time.time() - start_time
            
            if fixed_result['success']:
                print(f"   ✅ Синтаксис исправлен. Проверка прошла за {fix_time:.4f} сек")
                converted_script = fixed_script
            else:
                print(f"   ❌ Не удалось исправить синтаксис: {fixed_result['error']}")
    
    except Exception as e:
        print(f"   ❌ Ошибка при тестировании: {str(e)}")
    
    # Выводим сравнение
    display_script_comparison(script_content, converted_script)
    
    return converted_script

def main():
    """
    Основная функция для запуска тестирования
    """
    parser = argparse.ArgumentParser(description='Тестирование конвертера SQL скриптов')
    parser.add_argument('script', help='Путь к SQL скрипту для тестирования')
    parser.add_argument('--save', help='Сохранить сконвертированный скрипт в указанный файл')
    
    args = parser.parse_args()
    
    # Проверяем существование файла
    script_path = Path(args.script)
    if not script_path.exists() or not script_path.is_file():
        print(f"Ошибка: файл {script_path} не существует")
        return 1
    
    # Тестируем скрипт
    converted_script = test_script(args.script)
    
    # Сохраняем результат, если требуется
    if args.save:
        save_path = Path(args.save)
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(converted_script)
        print(f"\nСконвертированный скрипт сохранен в {save_path}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
