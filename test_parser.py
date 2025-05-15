#!/usr/bin/env python
"""
Скрипт для тестирования обработки параметров DOC.* в SQL-скриптах
"""

import sys
import os
import re
from pathlib import Path

# Добавляем путь к родительской директории для импорта
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Импортируем необходимые модули
from src.parser import SQLParser
import config

def test_param_replacement(script_content):
    """
    Проверяет корректность замены параметров в скрипте
    
    Args:
        script_content: Содержимое SQL скрипта
    """
    # Создаем экземпляр парсера
    parser = SQLParser(config)
    
    # Заменяем параметры
    result = parser.replace_params(script_content)
    
    # Выводим результат
    print("\n--- Исходный скрипт ---")
    print(script_content)
    print("\n--- После замены параметров ---")
    print(result)
    
    # Проверяем наличие параметров в результате
    if re.search(r'\{[^\}]+\}', result):
        print("\n⚠️ ВНИМАНИЕ: В результате всё ещё есть незаменённые параметры!")
        params = re.findall(r'\{([^\}]+)\}', result)
        print(f"Незаменённые параметры: {params}")
    else:
        print("\n✅ Все параметры успешно заменены")

def test_file(file_path):
    """
    Тестирует обработку параметров в указанном файле
    
    Args:
        file_path: Путь к файлу для тестирования
    """
    try:
        # Читаем содержимое файла
        with open(file_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Тестируем замену параметров
        test_param_replacement(script_content)
        
    except Exception as e:
        print(f"Ошибка при тестировании файла {file_path}: {str(e)}")

def test_example():
    """
    Тестирует обработку параметров на примере скрипта с DOC.*
    """
    example_script = """
CASE WHEN EXISTS(   SELECT 
                    doc.OUID
                    FROM WM_ACTDOCUMENTS doc
                    WHERE 	 doc.PERSONOUID = {params.childId}
                        AND  (doc.DOCUMENTSTYPE IN {DOC.birthDocs} or  doc.DOCUMENTSTYPE IN {DOC.dokORozhdeniiPechat}  or  doc.DOCUMENTSTYPE IN {DOC.dokFaktRozhdeniya}  or  doc.DOCUMENTSTYPE IN {DOC.DokFaktRozhdenInostran})
                        AND (doc.A_STATUS = {ACTIVESTATUS} OR doc.A_STATUS IS NULL)
                        AND {SQL.equalBeforeInDay(ISSUEEXTENSIONSDATE ,params.startDate)}
                        AND ({SQL.equalBeforeInDay(params.startDate,doc.COMPLETIONSACTIONDATE )}  OR doc.COMPLETIONSACTIONDATE IS NULL)
)
    THEN 1
    ELSE 0
END
    """
    
    test_param_replacement(example_script)

def main():
    """
    Основная функция скрипта
    """
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        # Если передан путь к файлу, тестируем его
        file_path = sys.argv[1]
        test_file(file_path)
    else:
        # Иначе тестируем пример
        test_example()

if __name__ == "__main__":
    main() 