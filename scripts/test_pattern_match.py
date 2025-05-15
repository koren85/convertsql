#!/usr/bin/env python
"""
Скрипт для тестирования определения паттернов, указывающих на необходимость 
ручной конвертации SQL скриптов
"""

import sys
import os
from pathlib import Path

# Добавляем путь к родительской директории для импорта
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Импортируем необходимые модули
from src.ai_converter import AIConverter
import config

def test_file(file_path: str) -> None:
    """
    Проверяет содержимое файла на наличие паттернов, указывающих 
    на необходимость ручной конвертации
    
    Args:
        file_path: Путь к файлу для проверки
    """
    try:
        # Создаем экземпляр конвертера
        converter = AIConverter(config)
        
        # Читаем содержимое файла
        with open(file_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Проверяем наличие паттернов
        should_skip, reason = converter.should_skip_conversion(script_content)
        
        # Выводим результат
        print(f"\nФайл: {file_path}")
        print("-" * 80)
        if should_skip:
            print(f"⚠️ СКРИПТ ТРЕБУЕТ РУЧНОЙ ОБРАБОТКИ: {reason}")
        else:
            print("✅ Скрипт может быть обработан автоматически")
        print("-" * 80)
        
    except Exception as e:
        print(f"Ошибка при проверке файла {file_path}: {str(e)}")

def main() -> None:
    """
    Основная функция скрипта
    """
    # Проверяем, передан ли аргумент с путем к файлу
    if len(sys.argv) < 2:
        print("Использование: python test_pattern_match.py <путь_к_sql_файлу>")
        print("Или: python test_pattern_match.py --all для проверки всех файлов в директории scripts/exported")
        return
    
    # Обрабатываем аргументы
    if sys.argv[1] == "--all":
        # Получаем директорию с экспортированными скриптами
        exported_dir = Path(__file__).resolve().parent / "exported"
        
        # Проверяем все файлы .sql в директории
        sql_files = list(exported_dir.glob("*.sql"))
        print(f"Найдено {len(sql_files)} SQL файлов для проверки")
        
        # Статистика
        need_manual = 0
        
        for file_path in sql_files:
            # Создаем экземпляр конвертера
            converter = AIConverter(config)
            
            # Читаем содержимое файла
            with open(file_path, 'r', encoding='utf-8') as f:
                script_content = f.read()
            
            # Проверяем наличие паттернов
            should_skip, reason = converter.should_skip_conversion(script_content)
            
            if should_skip:
                need_manual += 1
                print(f"⚠️ {file_path.name}: {reason}")
        
        # Выводим итоговую статистику
        print(f"\nИтого: {need_manual} из {len(sql_files)} скриптов требуют ручной обработки")
    else:
        # Проверяем один файл
        file_path = sys.argv[1]
        test_file(file_path)

if __name__ == "__main__":
    main() 