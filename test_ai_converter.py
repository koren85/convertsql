#!/usr/bin/env python3
"""
Скрипт для тестирования конвертации SQL с использованием нейросетей
"""

import os
import sys
import argparse
from pathlib import Path
import time
from dotenv import load_dotenv, find_dotenv

# Загружаем переменные из .env файла
env_path = find_dotenv(usecwd=True)
if env_path:
    load_dotenv(dotenv_path=env_path)
else:
    print("Файл .env не найден. Используются значения по умолчанию.")

# Импортируем наши модули
import config
from src.parser import SQLParser
from src.converter import SQLConverter
from src.postgres_tester import PostgresTester
from src.ai_converter import AIConverter
from src.logger import Logger

def test_ai_conversion(script_path, save_path=None, api_key=None, max_iterations=3):
    """
    Тестирует конвертацию SQL скрипта с использованием нейросети
    
    Args:
        script_path: Путь к SQL скрипту
        save_path: Путь для сохранения результата
        api_key: API ключ для нейросети
        max_iterations: Максимальное количество итераций для улучшения скрипта
    """
    print(f"Тестирование конвертации скрипта с использованием нейросети: {script_path}")
    
    # Создаем объекты для работы со скриптом
    parser = SQLParser(config)
    converter = SQLConverter(config)
    tester = PostgresTester(config)
    ai_converter = AIConverter(config)
    
    # Проверяем API ключи
    if api_key:
        # Если ключ API передан напрямую, используем его
        print(f"Используется ключ API, переданный в параметрах")
        # Обновляем API ключ в классе AIConverter
        if config.AI_PROVIDER == 'openai':
            ai_converter.api_keys['openai'] = api_key
            provider_name = 'OpenAI'
        elif config.AI_PROVIDER == 'anthropic':
            ai_converter.api_keys['anthropic'] = api_key
            provider_name = 'Anthropic'
    else:
        # Проверяем ключ API из .env
        if config.AI_PROVIDER == 'openai':
            api_key = os.getenv('OPENAI_API_KEY')
            provider_name = 'OpenAI'
        elif config.AI_PROVIDER == 'anthropic':
            api_key = os.getenv('ANTHROPIC_API_KEY')
            provider_name = 'Anthropic'
        else:
            provider_name = config.AI_PROVIDER
    
    if not api_key:
        print(f"Ошибка: API ключ для {provider_name} не найден")
        print(f"Добавьте ключ API в файл .env (см. .env.example) или передайте через параметр --api-key")
        return
    
    # Читаем содержимое скрипта
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
    
    print("\n1. Стандартная конвертация...")
    try:
        # Парсим и конвертируем скрипт стандартными методами
        parsed_script = parser.parse_script(script_content)
        standard_converted = converter.convert(parsed_script)
        standard_with_params = parser.replace_params(standard_converted)
        
        # Проверяем в PostgreSQL
        standard_result = tester.test_script(standard_with_params)
        
        if standard_result['success']:
            print("✅ Стандартная конвертация успешна!")
            if save_path:
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(standard_converted)
                print(f"Результат сохранен в {save_path}")
            return
        else:
            print(f"❌ Стандартная конвертация не удалась: {standard_result['error']}")
    except Exception as e:
        print(f"❌ Ошибка при стандартной конвертации: {str(e)}")
    
    print("\n2. Конвертация с помощью нейросети...")
    try:
        # Используем нейросеть для конвертации
        error_msg = standard_result['error'] if 'standard_result' in locals() else None
        success, ai_converted, message = ai_converter.convert_with_ai(script_content, error_msg, max_iterations)
        
        if success:
            print(f"Нейросеть: {message}")
            
            # Сохраняем результат нейросети, если указан путь, даже если тестирование не пройдено
            if save_path and ai_converted:
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(ai_converted)
                print(f"Результат работы нейросети сохранен в {save_path} (до проверки в PostgreSQL)")
            
            # Заменяем параметры
            ai_with_params = parser.replace_params(ai_converted)
            
            # Проверяем в PostgreSQL
            ai_result = tester.test_script(ai_with_params)
            
            if ai_result['success']:
                print("✅ Конвертация с помощью нейросети успешна!")
                if save_path:
                    print(f"Результат также сохранен в {save_path}")
                
                print("\nСравнение результатов:")
                print(f"- Стандартная конвертация: {'успешно' if 'standard_result' in locals() and standard_result['success'] else 'не удалось'}")
                print(f"- Конвертация с помощью нейросети: успешно")
                
                # Выводим конвертированный скрипт
                print("\n" + "="*80)
                print("СКОНВЕРТИРОВАННЫЙ СКРИПТ (нейросеть):")
                print("="*80)
                print(ai_converted)
                
                return
            else:
                print(f"❌ Конвертация с помощью нейросети не удалась: {ai_result['error']}")
        else:
            print(f"❌ Ошибка при использовании нейросети: {message}")
    except Exception as e:
        print(f"❌ Ошибка при конвертации с помощью нейросети: {str(e)}")
    
    print("\n❌ Конвертация не удалась ни стандартными методами, ни с помощью нейросети")

def main():
    """
    Основная функция для запуска тестирования
    """
    parser = argparse.ArgumentParser(description='Тестирование конвертации SQL с использованием нейросетей')
    parser.add_argument('script', help='Путь к SQL скрипту для тестирования')
    parser.add_argument('--save', help='Сохранить сконвертированный скрипт в указанный файл')
    parser.add_argument('--provider', choices=['openai', 'anthropic'], 
                      help='Провайдер нейросети (по умолчанию используется из конфигурации)')
    parser.add_argument('--env', help='Путь к .env файлу с настройками')
    parser.add_argument('--skip-docker-check', action='store_true', 
                      help='Пропустить проверку Docker (если Docker уже запущен)')
    parser.add_argument('--api-key', help='API ключ для провайдера нейросети (приоритетнее .env)')
    parser.add_argument('--no-real-db', action='store_true',
                      help='Не использовать реальную БД для тестирования (только синтаксическая проверка)')
    parser.add_argument('--max-iterations', type=int, default=3,
                      help='Максимальное количество итераций для улучшения скрипта (по умолчанию 3)')
    
    args = parser.parse_args()
    
    # Если указан путь к .env файлу, загружаем его
    if args.env:
        env_path = Path(args.env)
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
            print(f"Загружены настройки из файла: {env_path}")
        else:
            print(f"Файл .env не найден по пути: {env_path}")
            return 1
    
    # Проверяем существование файла
    script_path = Path(args.script)
    if not script_path.exists() or not script_path.is_file():
        print(f"Ошибка: файл {script_path} не существует")
        return 1
    
    # Устанавливаем провайдера, если указан
    if args.provider:
        config.AI_PROVIDER = args.provider
    
    # Устанавливаем режим тестирования без реальной БД, если указан флаг
    if args.no_real_db:
        config.USE_REAL_DB_TESTING = False
        print("Режим тестирования: только синтаксическая проверка (без реальной БД)")
    else:
        print(f"Режим тестирования: с использованием реальной БД ({config.PG_CONNECTION_STRING.replace(config.PG_CONFIG['password'], '****')})")
    
    # Проверяем Docker только если не указан флаг --skip-docker-check
    if not args.skip_docker_check:
        tester = PostgresTester(config)
        try:
            tester.ensure_docker_running()
            print("Docker контейнер с PostgreSQL запущен")
        except Exception as e:
            print(f"Ошибка при проверке Docker контейнера: {str(e)}")
            print("Запустите 'docker-compose up -d' перед использованием конвертера")
            return 1
    else:
        print("Проверка Docker пропущена. Предполагается, что контейнер уже запущен.")
    
    # Выводим информацию о настройках
    print(f"Используется нейросеть: {config.AI_PROVIDER}")
    print(f"Модель: {getattr(config, f'{config.AI_PROVIDER.upper()}_MODEL', 'не указана')}")
    
    # Запускаем тестирование
    test_ai_conversion(args.script, args.save, args.api_key, args.max_iterations)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
