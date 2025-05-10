#!/usr/bin/env python3
import os
import sys
import argparse
import traceback
from pathlib import Path
import time
from tqdm import tqdm
import concurrent.futures
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
from src.logger import Logger
from src.report_generator import ReportGenerator

def process_script(script_path, output_dir, config_obj, max_retry=3, use_ai=True):
    """
    Обрабатывает один SQL скрипт: парсит, конвертирует, тестирует и сохраняет
    
    Args:
        script_path: Путь к SQL скрипту
        output_dir: Директория для сохранения результата
        config_obj: Объект конфигурации
        max_retry: Максимальное количество попыток исправления
        use_ai: Использовать ли нейросеть при необходимости
    """
    script_name = os.path.basename(script_path)
    
    # Создаем объекты для работы со скриптом
    parser = SQLParser(config_obj)
    converter = SQLConverter(config_obj)
    tester = PostgresTester(config_obj)
    logger = Logger(config_obj)
    
    try:
        # Читаем содержимое скрипта
        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Логируем начало обработки
        logger.log_script_processing(script_name, 'start', 'success')
        
        # Парсим скрипт
        try:
            parsed_script = parser.parse_script(script_content)
            logger.log_script_processing(script_name, 'parsing', 'success')
        except Exception as e:
            logger.log_script_processing(script_name, 'parsing', 'failed', str(e))
            return False
        
        # Конвертируем скрипт
        try:
            converted_script = converter.convert(parsed_script)
            logger.log_script_processing(script_name, 'conversion', 'success')
        except Exception as e:
            logger.log_script_processing(script_name, 'conversion', 'failed', str(e))
            return False
        
        # Заменяем параметры на значения по умолчанию
        try:
            script_with_params = parser.replace_params(converted_script)
            logger.log_script_processing(script_name, 'parameter_replacement', 'success')
        except Exception as e:
            logger.log_script_processing(script_name, 'parameter_replacement', 'failed', str(e))
            return False
        
        # Тестируем в PostgreSQL
        retry_count = 0
        last_error = None
        ai_used = False
        
        # Порог использования AI - после скольких попыток переключаться на AI
        ai_fallback_threshold = getattr(config_obj, 'AI_FALLBACK_THRESHOLD', max_retry - 1)
        
        while retry_count < max_retry:
            try:
                test_result = tester.test_script(script_with_params)
                if test_result['success']:
                    logger.log_script_processing(script_name, 'testing', 'success', {
                        'execution_time': test_result['execution_time'],
                        'row_count': test_result['row_count'],
                        'ai_used': ai_used
                    })
                    break
                else:
                    last_error = test_result['error']
                    
                    # Логируем ошибку
                    logger.log_script_processing(script_name, f'attempt_{retry_count+1}', 'failed', {
                        'error': last_error
                    })
                    
                    # Если достигли порога использования AI и AI включен,
                    # пробуем использовать нейросеть
                    if (retry_count >= ai_fallback_threshold and 
                        use_ai and 
                        hasattr(config_obj, 'USE_AI_CONVERSION') and 
                        config_obj.USE_AI_CONVERSION):
                        
                        print(f"Используем нейросеть для конвертации {script_name}...")
                        logger.log_script_processing(script_name, 'ai_conversion', 'started')
                        
                        # Пытаемся исправить с помощью нейросети, передавая оригинальный скрипт
                        converted_script = tester.fix_script(converted_script, last_error, 
                                                           script_content, use_ai=True)
                        script_with_params = parser.replace_params(converted_script)
                        ai_used = True
                        
                        logger.log_script_processing(script_name, 'ai_conversion', 'completed')
                    else:
                        # Пытаемся исправить стандартными методами
                        converted_script = tester.fix_script(converted_script, last_error, 
                                                           use_ai=False)
                        script_with_params = parser.replace_params(converted_script)
                    
                    retry_count += 1
            except Exception as e:
                last_error = str(e)
                logger.log_script_processing(script_name, f'attempt_{retry_count+1}', 'error', {
                    'error': last_error
                })
                retry_count += 1
        
        if retry_count == max_retry:
            logger.log_script_processing(script_name, 'testing', 'failed', {
                'error': last_error,
                'ai_used': ai_used
            })
            # Все равно сохраняем скрипт, даже если он не прошел тесты
        
        # Сохраняем сконвертированный скрипт
        output_path = os.path.join(output_dir, script_name)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(converted_script)
                
            # Дополнительно сохраняем версию с подставленными параметрами
            params_output_path = os.path.join(output_dir, f"params_{script_name}")
            with open(params_output_path, 'w', encoding='utf-8') as f:
                f.write(script_with_params)
                
            logger.log_script_processing(script_name, 'saving', 'success', {
                'output_path': output_path,
                'params_output_path': params_output_path
            })
            
            return retry_count < max_retry  # Возвращаем True, если тест успешен
        except Exception as e:
            logger.log_script_processing(script_name, 'saving', 'failed', str(e))
            return False
            
    except Exception as e:
        logger.log_script_processing(script_name, 'processing', 'failed', f"{str(e)}\n{traceback.format_exc()}")
        return False

def main():
    """
    Основная функция для запуска конвертации
    """
    parser = argparse.ArgumentParser(description='Конвертер SQL скриптов из MS SQL в PostgreSQL')
    parser.add_argument('input', help='Путь к директории с исходными скриптами или к одному скрипту')
    parser.add_argument('--output', default=None, help='Путь к директории для сохранения результатов')
    parser.add_argument('--parallel', type=int, default=4, help='Количество параллельных потоков для обработки')
    parser.add_argument('--report', action='store_true', help='Генерировать отчет после завершения')
    parser.add_argument('--max-retry', type=int, default=3, help='Максимальное количество попыток исправления ошибок')
    parser.add_argument('--use-ai', action='store_true', help='Использовать нейросеть для сложных случаев')
    parser.add_argument('--no-ai', action='store_true', help='Не использовать нейросеть даже если она включена в конфигурации')
    parser.add_argument('--ai-provider', choices=['openai', 'anthropic'], help='Указать провайдера нейросети')
    parser.add_argument('--env', help='Путь к .env файлу с настройками')
    
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
    
    # Определяем пути к директориям
    input_path = Path(args.input)
    output_dir = Path(args.output) if args.output else config.CONVERTED_DIR
    
    # Применяем параметры командной строки к конфигурации
    if args.use_ai:
        config.USE_AI_CONVERSION = True
    elif args.no_ai:
        config.USE_AI_CONVERSION = False
        
    if args.ai_provider:
        config.AI_PROVIDER = args.ai_provider
    
    # Убеждаемся, что выходная директория существует
    os.makedirs(output_dir, exist_ok=True)
    
    # Получаем список скриптов для обработки
    if input_path.is_file():
        scripts = [input_path]
    elif input_path.is_dir():
        scripts = list(input_path.glob('*.sql'))
    else:
        print(f"Путь не существует: {input_path}")
        return 1
    
    print(f"Найдено {len(scripts)} SQL скриптов для обработки")
    
    # Проверяем API ключи для нейросетей, если они используются
    if config.USE_AI_CONVERSION:
        openai_key = os.getenv('OPENAI_API_KEY')
        anthropic_key = os.getenv('ANTHROPIC_API_KEY')
        
        if config.AI_PROVIDER == 'openai' and not openai_key:
            print("Предупреждение: OPENAI_API_KEY не найден в .env файле или переменных окружения")
            print("Нейросеть OpenAI не будет использоваться.")
            print("Создайте файл .env на основе .env.example и укажите в нем ключ API.")
            config.USE_AI_CONVERSION = False
            
        if config.AI_PROVIDER == 'anthropic' and not anthropic_key:
            print("Предупреждение: ANTHROPIC_API_KEY не найден в .env файле или переменных окружения")
            print("Нейросеть Anthropic не будет использоваться.")
            print("Создайте файл .env на основе .env.example и укажите в нем ключ API.")
            config.USE_AI_CONVERSION = False
        
        if config.USE_AI_CONVERSION:
            print(f"Используется нейросеть: {config.AI_PROVIDER}")
            print(f"Модель: {getattr(config, f'{config.AI_PROVIDER.upper()}_MODEL', 'не указана')}")
    
    # Выводим информацию о настройках PostgreSQL
    print(f"Подключение к PostgreSQL: {config.PG_CONFIG['host']}:{config.PG_CONFIG['port']}")
    
    # Проверяем, запущен ли Docker с PostgreSQL
    tester = PostgresTester(config)
    try:
        tester.ensure_docker_running()
    except Exception as e:
        print(f"Ошибка при проверке Docker контейнера: {str(e)}")
        print("Запустите 'docker-compose up -d' перед использованием конвертера")
        return 1
    
    # Обрабатываем скрипты
    start_time = time.time()
    successful = 0
    failed = 0
    
    # Используем пул потоков для параллельной обработки
    use_ai = not args.no_ai
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as executor:
        # Запускаем обработку всех скриптов
        future_to_script = {
            executor.submit(process_script, str(script), str(output_dir), 
                         config, args.max_retry, use_ai): script
            for script in scripts
        }
        
        # Отображаем прогресс
        for future in tqdm(concurrent.futures.as_completed(future_to_script), 
                         total=len(scripts), 
                         desc="Конвертация скриптов"):
            script = future_to_script[future]
            try:
                result = future.result()
                if result:
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"Ошибка при обработке {script}: {str(e)}")
                failed += 1
    
    # Выводим итоги
    elapsed_time = time.time() - start_time
    print(f"Обработка завершена за {elapsed_time:.2f} секунд")
    print(f"Успешно обработано: {successful}")
    print(f"Не удалось обработать: {failed}")
    
    # Генерируем отчет, если требуется
    if args.report:
        try:
            report_generator = ReportGenerator(config)
            reports = report_generator.generate_summary_report()
            print(f"Отчеты созданы:")
            for report_type, report_path in reports.items():
                print(f"  - {report_type}: {report_path}")
        except Exception as e:
            print(f"Ошибка при генерации отчета: {str(e)}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
