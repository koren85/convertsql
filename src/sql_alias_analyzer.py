"""
Модуль для анализа SQL скриптов и определения алиасов таблиц.
Использует sqlglot для более точного анализа, с запасными методами при необходимости.
"""

import re
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Set

# Пытаемся импортировать sqlglot
SQLGLOT_AVAILABLE = False
try:
    import sqlglot
    from sqlglot import parse as sqlglot_parse
    SQLGLOT_AVAILABLE = True
except ImportError:
    print("Библиотека sqlglot не установлена. Будет использоваться запасной метод анализа алиасов.")

class SQLAliasAnalyzer:
    """
    Анализатор SQL скриптов для определения алиасов таблиц и их связей.
    Поддерживает основной метод с использованием sqlglot и запасные методы с регулярными выражениями.
    """
    
    def __init__(self):
        """Инициализация анализатора"""
        # Кэш алиасов для скриптов, чтобы не перепарсить один и тот же скрипт
        self.alias_cache = {}
        
    def get_table_aliases(self, sql_script: str) -> Dict[str, List[str]]:
        """
        Получает соответствие таблиц и их алиасов из SQL скрипта.
        
        Args:
            sql_script: SQL скрипт для анализа
            
        Returns:
            Dict[str, List[str]]: Словарь {имя_таблицы: [список_алиасов]}
        """
        # Проверяем, есть ли результат в кэше
        script_hash = hash(sql_script)
        if script_hash in self.alias_cache:
            return self.alias_cache[script_hash]
            
        # Основной метод - использование sqlglot если доступен
        if SQLGLOT_AVAILABLE:
            try:
                aliases = self._extract_aliases_with_sqlglot(sql_script)
                # Если нет ошибок, сохраняем результат в кэш и возвращаем
                if "Error" not in aliases:
                    self.alias_cache[script_hash] = aliases
                    return aliases
            except Exception as e:
                print(f"Ошибка при разборе SQL с sqlglot: {e}")
                # Продолжаем с запасным методом
        
        # Запасной метод - регулярные выражения
        aliases = self._extract_aliases_with_regex(sql_script)
        self.alias_cache[script_hash] = aliases
        return aliases
    
    def get_table_by_alias(self, sql_script: str, alias: str) -> Optional[str]:
        """
        Определяет таблицу по алиасу в SQL скрипте.
        
        Args:
            sql_script: SQL скрипт для анализа
            alias: Алиас таблицы
            
        Returns:
            Optional[str]: Имя таблицы или None, если алиас не найден
        """
        table_aliases = self.get_table_aliases(sql_script)
        
        # Приводим алиас к нижнему регистру для сравнения
        alias_lower = alias.lower()
        
        # Ищем совпадение в словаре алиасов
        for table, aliases in table_aliases.items():
            # Приводим список алиасов к нижнему регистру
            aliases_lower = [a.lower() for a in aliases]
            if alias_lower in aliases_lower:
                return table
        
        return None
    
    def _extract_aliases_with_sqlglot(self, sql_script: str) -> Dict[str, List[str]]:
        """
        Извлечение алиасов таблиц с помощью библиотеки sqlglot
        
        Args:
            sql_script: SQL скрипт для анализа
            
        Returns:
            Dict[str, List[str]]: Словарь соответствия имен таблиц их алиасам
        """
        if not SQLGLOT_AVAILABLE:
            return {"Error": "Библиотека sqlglot не установлена"}
        
        try:
            # Разбор SQL скрипта
            parsed = sqlglot_parse(sql_script)
            
            # Словарь для хранения результатов (таблица -> список алиасов)
            table_aliases = defaultdict(list)
            
            # Извлечение всех ссылок на таблицы с их алиасами
            for statement in parsed:
                tables = statement.find_all(sqlglot.exp.Table)
                
                for table in tables:
                    table_name = table.name
                    alias = table.alias
                    
                    if alias and alias not in table_aliases[table_name]:
                        table_aliases[table_name].append(alias)
            
            # Преобразуем в обычный словарь
            return dict(table_aliases)
        except Exception as e:
            print(f"Ошибка разбора SQL с помощью sqlglot: {e}")
            return {"Error": f"Ошибка разбора SQL с помощью sqlglot: {e}"}
    
    def _extract_aliases_with_regex(self, sql_script: str) -> Dict[str, List[str]]:
        """
        Извлечение алиасов таблиц с помощью регулярных выражений (запасной метод)
        
        Args:
            sql_script: SQL скрипт для анализа
            
        Returns:
            Dict[str, List[str]]: Словарь соответствия имен таблиц их алиасам
        """
        # Словарь для хранения результатов (таблица -> список алиасов)
        table_aliases = defaultdict(list)
        
        # Регулярные выражения для обнаружения таблиц и алиасов
        # Шаблон 1: "имя_таблицы AS алиас" или "имя_таблицы алиас"
        pattern1 = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)\s+(?:AS\s+)?([a-zA-Z0-9_]+)'
        
        # Шаблон 2: Для подзапросов с алиасами
        pattern2 = r'\)\s+(?:AS\s+)?([a-zA-Z0-9_]+)'
        
        # Находим все совпадения по шаблону 1
        for match in re.finditer(pattern1, sql_script, re.IGNORECASE):
            table, alias = match.groups()
            if alias not in table_aliases[table]:
                table_aliases[table].append(alias)
        
        # Находим подзапросы с алиасами
        for match in re.finditer(pattern2, sql_script, re.IGNORECASE):
            alias = match.group(1)
            # Для подзапросов используем специальный ключ
            table_aliases["SUBQUERY"].append(alias)
        
        # Ищем все таблицы без алиасов
        tables_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)\s+(?:WHERE|ON|GROUP|ORDER|HAVING|UNION|INTERSECT|EXCEPT|$)'
        for match in re.finditer(tables_pattern, sql_script, re.IGNORECASE):
            table = match.group(1)
            # Если таблица еще не в словаре и не является частью ключевых слов SQL
            if table not in table_aliases and not any(kw == table.upper() for kw in ['SELECT', 'WHERE', 'GROUP', 'ORDER']):
                table_aliases[table] = []
        
        # Преобразуем в обычный словарь
        return dict(table_aliases)
    
    def get_column_to_table_mapping(self, sql_script: str) -> Dict[str, str]:
        """
        Определяет соответствие колонок таблицам на основе алиасов
        
        Args:
            sql_script: SQL скрипт для анализа
            
        Returns:
            Dict[str, str]: Словарь {полное_имя_колонки: имя_таблицы}
        """
        # Получаем алиасы таблиц
        table_aliases = self.get_table_aliases(sql_script)
        
        # Словарь соответствия {алиас: таблица}
        alias_to_table = {}
        for table, aliases in table_aliases.items():
            for alias in aliases:
                alias_to_table[alias.lower()] = table
        
        # Словарь соответствия {полное_имя_колонки: имя_таблицы}
        column_to_table = {}
        
        # Находим все колонки с алиасами в скрипте
        column_pattern = r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
        for match in re.finditer(column_pattern, sql_script):
            alias, column = match.groups()
            alias_lower = alias.lower()
            
            # Если алиас найден, добавляем сопоставление колонки с таблицей
            if alias_lower in alias_to_table:
                full_column_name = f"{alias}.{column}"
                column_to_table[full_column_name] = alias_to_table[alias_lower]
        
        return column_to_table 