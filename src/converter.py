import re
import sqlparse

class SQLConverter:
    def __init__(self, config):
        self.config = config
        self.data_type_mapping = config.DATA_TYPE_MAPPING
        self.function_mapping = config.FUNCTION_MAPPING
        
    def convert(self, parsed_script):
        """
        Конвертирует скрипт MS SQL в скрипт PostgreSQL
        """
        converted_script = parsed_script['original']
        
        # Преобразование синтаксиса
        converted_script = self._convert_data_types(converted_script)
        converted_script = self._convert_functions(converted_script)
        converted_script = self._convert_joins(converted_script)
        converted_script = self._convert_top_to_limit(converted_script)
        converted_script = self._convert_brackets_to_quotes(converted_script)
        converted_script = self._convert_schemas(converted_script)
        converted_script = self._convert_date_formats(converted_script)
        converted_script = self._convert_ctes(converted_script)
        converted_script = self._convert_case_statements(converted_script)
        
        return converted_script
    
    def _convert_data_types(self, script):
        """Конвертирует типы данных из MS SQL в PostgreSQL"""
        for ms_type, pg_type in self.data_type_mapping.items():
            # Используем регулярное выражение для замены типов данных
            script = re.sub(fr'\b{ms_type}\b', pg_type, script, flags=re.IGNORECASE)
        return script
    
    def _convert_functions(self, script):
        """Конвертирует встроенные функции из MS SQL в PostgreSQL"""
        for ms_func, pg_func in self.function_mapping.items():
            # Простая замена функций
            script = script.replace(ms_func, pg_func)
            
        # Специальная обработка CONVERT
        script = re.sub(r'CONVERT\s*\(\s*([^,]+)\s*,\s*([^,\)]+)(?:\s*,\s*[^\)]+)?\s*\)', 
                       r'CAST(\2 AS \1)', script, flags=re.IGNORECASE)
        
        # Специальная обработка DATEADD
        script = re.sub(r'DATEADD\s*\(\s*([^,]+)\s*,\s*([^,\)]+)\s*,\s*([^,\)]+)\s*\)', 
                       r'\3 + INTERVAL \'\2 \1\'', script, flags=re.IGNORECASE)
        
        # Специальная обработка DATEDIFF
        script = re.sub(r'DATEDIFF\s*\(\s*([^,]+)\s*,\s*([^,\)]+)\s*,\s*([^,\)]+)\s*\)', 
                       r'EXTRACT(EPOCH FROM (\3 - \2))/(CASE \'\1\' WHEN \'SECOND\' THEN 1 WHEN \'MINUTE\' THEN 60 WHEN \'HOUR\' THEN 3600 WHEN \'DAY\' THEN 86400 WHEN \'WEEK\' THEN 604800 WHEN \'MONTH\' THEN 2592000 WHEN \'YEAR\' THEN 31536000 END)', 
                       script, flags=re.IGNORECASE)
        
        return script
    
    def _convert_joins(self, script):
        """Конвертирует синтаксис JOIN из MS SQL в PostgreSQL"""
        # Заменить синтаксис =* и *= на LEFT JOIN и RIGHT JOIN
        script = re.sub(r'(\w+\.\w+)\s*=\*\s*(\w+\.\w+)', r'LEFT JOIN \2 ON \1 = \2', script)
        script = re.sub(r'(\w+\.\w+)\s*\*=\s*(\w+\.\w+)', r'RIGHT JOIN \1 ON \1 = \2', script)
        return script
    
    def _convert_top_to_limit(self, script):
        """Конвертирует TOP в LIMIT"""
        # Находим все вхождения TOP и преобразуем их в LIMIT
        script = re.sub(r'SELECT\s+TOP\s+(\d+)', r'SELECT', script, flags=re.IGNORECASE)
        
        # Обработка TOP с переменными или параметрами
        script = re.sub(r'SELECT\s+TOP\s+\(\s*([^\s]+)\s*\)', r'SELECT', script, flags=re.IGNORECASE)
        
        # Добавляем LIMIT в конец запроса, если нашли TOP
        top_matches = re.finditer(r'SELECT\s+TOP\s+(\d+)|\(\s*([^\s]+)\s*\)', script, flags=re.IGNORECASE)
        for match in top_matches:
            limit_value = match.group(1) if match.group(1) else match.group(2)
            # Это упрощенный подход, для реального использования нужно учитывать сложность запросов
            script = re.sub(r';?\s*$', f' LIMIT {limit_value};', script)
            
        return script
    
    def _convert_brackets_to_quotes(self, script):
        """Заменяет квадратные скобки на двойные кавычки для идентификаторов"""
        script = re.sub(r'\[([^\]]+)\]', r'"\1"', script)
        return script
    
    def _convert_schemas(self, script):
        """Конвертирует схемы из MS SQL в PostgreSQL"""
        # В PostgreSQL схемы также поддерживаются, просто сохраняем их формат
        return script
    
    def _convert_date_formats(self, script):
        """Конвертирует форматы дат из MS SQL в PostgreSQL"""
        # Заменяем формат даты в стиле MS SQL на PostgreSQL
        script = re.sub(r"'(\d{2})/(\d{2})/(\d{4})'", r"'\3-\1-\2'", script)
        return script
    
    def _convert_ctes(self, script):
        """Конвертирует общие табличные выражения из MS SQL в PostgreSQL"""
        # В PostgreSQL CTE поддерживаются аналогично MS SQL
        return script
    
    def _convert_case_statements(self, script):
        """Конвертирует операторы CASE из MS SQL в PostgreSQL"""
        # В PostgreSQL операторы CASE поддерживаются аналогично MS SQL
        return script
