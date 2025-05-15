import sys
import os
import pytest
from pathlib import Path

# Добавляем путь к пакету src для импорта 
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Импортируем нужные модули
from src.ai_converter import AIConverter
import config

class TestAIConverter:
    """Тесты для класса AIConverter"""
    
    @pytest.fixture
    def converter(self):
        """Фикстура, создающая экземпляр конвертера с тестовой конфигурацией"""
        return AIConverter(config)
    
    def test_should_skip_conversion_case_when(self, converter):
        """Тест на скрипт, начинающийся с CASE WHEN"""
        script = """
        CASE WHEN EXISTS(SELECT * FROM test_table)
            THEN 1
            ELSE 0
        END
        """
        should_skip, reason = converter.should_skip_conversion(script)
        assert should_skip is True
        assert "CASE WHEN" in reason
    
    def test_should_skip_conversion_sql_equal_before_in_day(self, converter):
        """Тест на скрипт, содержащий SQL.equalBeforeInDay"""
        script = """
        SELECT * FROM table
        WHERE {SQL.equalBeforeInDay(date1, date2)}
        """
        should_skip, reason = converter.should_skip_conversion(script)
        assert should_skip is True
        assert "SQL.equalBeforeInDay" in reason
    
    def test_should_skip_conversion_doc_params(self, converter):
        """Тест на скрипт, содержащий динамические параметры DOC.*"""
        script = """
        SELECT * FROM table
        WHERE column IN {DOC.birthDocs}
        """
        should_skip, reason = converter.should_skip_conversion(script)
        assert should_skip is True
        assert "DOC." in reason
    
    def test_should_skip_conversion_combined_case(self, converter):
        """Тест на скрипт, содержащий несколько паттернов"""
        script = """
        CASE WHEN EXISTS(
            SELECT * FROM table
            WHERE column IN {DOC.birthDocs}
            AND {SQL.equalBeforeInDay(date1, date2)}
        )
            THEN 1
            ELSE 0
        END
        """
        should_skip, reason = converter.should_skip_conversion(script)
        assert should_skip is True
        # В этом тесте причиной будет первое совпадение
        assert "CASE WHEN" in reason
    
    def test_should_not_skip_normal_script(self, converter):
        """Тест на обычный скрипт, который не должен пропускаться"""
        script = """
        SELECT * FROM table
        WHERE column = 1
        """
        should_skip, reason = converter.should_skip_conversion(script)
        assert should_skip is False
        assert reason == ""
    
    # Здесь могут быть другие тесты для класса AIConverter 