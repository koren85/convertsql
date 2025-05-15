#!/usr/bin/env python
"""
Скрипт для тестирования обнаружения SQL.* функций в скриптах
"""

import sys
import os
import re
from pathlib import Path

# Добавляем путь к родительской директории для импорта
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Импортируем необходимые модули
from src.ai_converter import AIConverter
import config

def test_script(script_content, script_name="Тестовый скрипт"):
    """
    Проверяет скрипт на наличие SQL.* функций
    
    Args:
        script_content: Содержимое скрипта
        script_name: Название скрипта для вывода
    """
    # Создаем экземпляр конвертера
    converter = AIConverter(config)
    
    # Проверяем скрипт
    should_skip, reason = converter.should_skip_conversion(script_content)
    
    # Выводим результат
    print(f"\n--- {script_name} ---")
    print(script_content)
    print("\n--- Результат проверки ---")
    if should_skip:
        print(f"⚠️ СКРИПТ ТРЕБУЕТ РУЧНОЙ ОБРАБОТКИ: {reason}")
    else:
        print("✅ Скрипт может быть обработан автоматически")
    print("-" * 80)

def main():
    """
    Основная функция скрипта
    """
    # Проверяем разные скрипты
    
    # 1. Скрипт с SQL.equalBeforeInDay
    script1 = """
    SELECT * FROM table
    WHERE {SQL.equalBeforeInDay(startDate, endDate)}
    """
    test_script(script1, "Скрипт с SQL.equalBeforeInDay")
    
    # 2. Скрипт с SQL.endMonth
    script2 = """
    SELECT * FROM table
    WHERE date = {SQL.endMonth(currentDate)}
    """
    test_script(script2, "Скрипт с SQL.endMonth")
    
    # 3. Скрипт с SQL.addYear
    script3 = """
    SELECT * FROM table
    WHERE date = {SQL.addYear(currentDate, 1)}
    """
    test_script(script3, "Скрипт с SQL.addYear")
    
    # 4. Скрипт с SQL.getDate
    script4 = """
    SELECT * FROM table
    WHERE date = {SQL.getDate()}
    """
    test_script(script4, "Скрипт с SQL.getDate")
    
    # 5. Скрипт с SQL.getYearOld
    script5 = """
    SELECT * FROM table
    WHERE age = {SQL.getYearOld(birthDate)}
    """
    test_script(script5, "Скрипт с SQL.getYearOld")
    
    # 6. Скрипт с произвольной SQL.* функцией
    script6 = """
    SELECT * FROM table
    WHERE value = {SQL.someCustomFunction(param1, param2)}
    """
    test_script(script6, "Скрипт с произвольной SQL.* функцией")
    
    # 7. Обычный скрипт без SQL.* функций
    script7 = """
    SELECT * FROM table
    WHERE date = '2023-01-01'
    """
    test_script(script7, "Обычный скрипт без SQL.* функций")

if __name__ == "__main__":
    main() 