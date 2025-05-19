#!/usr/bin/env python3
"""
Скрипт для пакетной обработки SQL-скриптов с различными настройками
"""

import os
import sys
import argparse
import yaml
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Импортируем наши модули
import config
from src.parser import SQLParser
from src.converter import SQLConverter
from src.postgres_tester import PostgresTester
from src.logger import Logger
from src.report_generator import ReportGenerator
from src.ai_converter import AIConverter

def process_script(script_path, output_dir, params=None, retry_count=3, verbose=False, ai_provider='anthropic', max_iterations=3):
    """
    Обрабатывает один SQL скрипт с заданными параметрами
    """
    script_name = "conv_" + os.path.basename(script_path)
    
    # Создаем объекты для работы со скриптом
    parser = SQLParser(config)
    converter = AIConverter(config)
    tester = PostgresTester(config)
    logger = Logger(config)
    
    try:
        # Читаем содержимое скрипта
        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Логируем начало обработки
        # logger.log_script_processing(script_name, 'start', 'success')
        
        # Парсим скрипт
        parsed_script = parser.parse_script(script_content)
        # logger.log_script_processing(script_name, 'parsing', 'success')
        
        # Конвертация через нейросеть
        # Временно подменяем config.AI_PROVIDER
        orig_provider = getattr(config, 'AI_PROVIDER', None)
        config.AI_PROVIDER = ai_provider
        success, converted_script, message = converter.convert_with_ai(parsed_script, error_message=None, max_iterations=max_iterations)
        if orig_provider is not None:
            config.AI_PROVIDER = orig_provider
            
        # Проверяем, требуется ли ручная обработка
        requires_manual = False
        if not success and "требует ручной обработки" in message:
            requires_manual = True
            if verbose:
                print(f"⚠️ {script_name}: {message}")
            logger.log_script_processing(script_name, 'conversion', 'skipped', message)
            return {
                'script': script_name,
                'success': False,
                'error': message,
                'manual_processing': True,
                'retries': 0,
                'original_size': len(script_content),
                'converted_size': len(converted_script)
            }
            
        # logger.log_script_processing(script_name, 'conversion', 'success' if success else 'failed', message)
        
        # Заменяем параметры на значения
        script_params = config.DEFAULT_PARAMS.copy()
        if params:
            script_params.update(params)
            
        script_with_params = parser.replace_params(converted_script, script_params)
        # logger.log_script_processing(script_name, 'parameter_replacement', 'success')
        
        # Тестируем в PostgreSQL
        retries = 0
        last_error = None
        test_success = False
        missing_table = False
        
        while retries < retry_count and not test_success:
            try:
                test_result = tester.test_script(script_with_params)
                if test_result['success']:
                    test_success = True
                    # logger.log_script_processing(script_name, 'testing', 'success', {
                    #     'execution_time': test_result['execution_time'],
                    #     'row_count': test_result['row_count']
                    # })
                else:
                    last_error = test_result['error']
                    # Проверяем, содержит ли ошибка сообщение о несуществующей таблице
                    if 'relation' in last_error and 'does not exist' in last_error:
                        missing_table = True
                        # Прекращаем повторные попытки, так как таблицы всё равно нет
                        break
                    
                    if verbose:
                        print(f"Попытка {retries+1}: Ошибка выполнения {script_name}: {last_error}")
                    # Не вызываем fix_script, AI уже делал исправления
                    retries += 1
            except Exception as e:
                last_error = str(e)
                # Проверяем, содержит ли исключение сообщение о несуществующей таблице
                if 'relation' in str(e) and 'does not exist' in str(e):
                    missing_table = True
                    # Прекращаем повторные попытки, так как таблицы всё равно нет
                    break
                
                if verbose:
                    print(f"Попытка {retries+1}: Исключение при выполнении {script_name}: {last_error}")
                retries += 1
        
        # Лог только если ошибка
        if missing_table:
            logger.log_script_processing(script_name, 'error', 'missing_table', last_error)
            if verbose:
                print(f"📋 {script_name}: Пропущен из-за отсутствия таблицы: {last_error}")
        elif not test_success or not success:
            logger.log_script_processing(script_name, 'error', 'failed', last_error or message)
            if verbose:
                print(f"❌ {script_name}: Не удалось выполнить после {retry_count} попыток")
        elif verbose:
            print(f"✅ {script_name}: Успешно сконвертирован и выполнен")
        
        # Сохраняем сконвертированный скрипт
        output_path = os.path.join(output_dir, script_name)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(converted_script)
        # logger.log_script_processing(script_name, 'saving', 'success')
        
        return {
            'script': script_name,
            'success': test_success and success,
            'error': last_error or message if not test_success or not success else None,
            'manual_processing': requires_manual,
            'missing_table': missing_table,
            'retries': retries,
            'original_size': len(script_content),
            'converted_size': len(converted_script)
        }
            
    except Exception as e:
        error_msg = str(e)
        logger.log_script_processing(script_name, 'processing', 'failed', error_msg)
        if verbose:
            print(f"❌ {script_name}: Ошибка обработки: {error_msg}")
        
        return {
            'script': script_name,
            'success': False,
            'error': error_msg,
            'manual_processing': False,
            'retries': 0,
            'original_size': 0,
            'converted_size': 0
        }

def process_batch(config_file, verbose=False, ai_provider='anthropic', skip_docker_check=False, max_iterations=3, limit=None, offset=0):
    """
    Обрабатывает пакет скриптов по конфигурации
    """
    # Загружаем конфигурацию
    try:
        with open(config_file, 'r') as f:
            batch_config = yaml.safe_load(f)
    except Exception as e:
        print(f"Ошибка при загрузке конфигурации: {str(e)}")
        return False
    
    # Проверяем обязательные поля
    required_fields = ['name', 'input_dir', 'output_dir']
    for field in required_fields:
        if field not in batch_config:
            print(f"Ошибка: в конфигурации отсутствует обязательное поле '{field}'")
            return False
    
    # Получаем параметры
    batch_name = batch_config['name']
    input_dir = Path(batch_config['input_dir'])
    output_dir = Path(batch_config['output_dir'])
    retry_count = batch_config.get('retry_count', 3)
    parallel = batch_config.get('parallel', 4)
    params = batch_config.get('params', {})
    limit = limit or batch_config.get('limit')
    offset = offset or batch_config.get('offset', 0)
    
    print(f"Запуск пакетной обработки: {batch_name}")
    print(f"Исходная директория: {input_dir}")
    print(f"Выходная директория: {output_dir}")
    print(f"Количество повторных попыток: {retry_count}")
    print(f"Параллельных потоков: {parallel}")
    if params:
        print(f"Пользовательские параметры: {json.dumps(params, indent=2)}")
    
    # Проверяем существование директорий
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Ошибка: директория {input_dir} не существует")
        return False
    
    # Создаем выходную директорию
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Находим все SQL-скрипты и сортируем по имени
    scripts = sorted(input_dir.glob('*.sql'), key=lambda p: p.name)
    
    # Применяем offset (пропускаем первые N файлов)
    if offset > 0:
        scripts = scripts[offset:]
        print(f"Пропущено первых {offset} файлов")
    
    # Применяем limit (ограничиваем количество)    
    if limit is not None:
        scripts = scripts[:limit]
        
    if not scripts:
        print(f"В директории {input_dir} не найдено SQL-скриптов после применения offset/limit")
        return False
    
    print(f"Найдено {len(scripts)} SQL-скриптов для обработки")
    
    # Проверяем Docker
    tester = PostgresTester(config)
    if not skip_docker_check:
        try:
            tester.ensure_docker_running()
            print("Docker контейнер с PostgreSQL запущен")
        except Exception as e:
            print(f"Ошибка при проверке Docker контейнера: {str(e)}")
            print("Запустите 'docker-compose up -d' перед использованием скрипта")
            return False
    
    # Обрабатываем скрипты параллельно
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        # Запускаем обработку всех скриптов
        future_to_script = {
            executor.submit(process_script, str(script), str(output_dir), params, retry_count, verbose, ai_provider, max_iterations): script
            for script in scripts
        }
        
        # Отображаем прогресс
        for future in tqdm(as_completed(future_to_script), total=len(scripts), desc="Обработка скриптов"):
            script = future_to_script[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Ошибка при обработке {script}: {str(e)}")
                results.append({
                    'script': os.path.basename(str(script)),
                    'success': False,
                    'error': str(e),
                    'retries': 0,
                    'original_size': 0,
                    'converted_size': 0
                })
    
    # Выводим итоги
    elapsed_time = time.time() - start_time
    success_count = sum(1 for r in results if r['success'])
    manual_count = sum(1 for r in results if r.get('manual_processing', False))
    missing_table_count = sum(1 for r in results if r.get('missing_table', False))
    failed_count = len(results) - success_count - manual_count - missing_table_count
    
    print(f"\nОбработка завершена за {elapsed_time:.2f} секунд")
    print(f"Всего обработано: {len(results)} скриптов")
    print(f"  - Успешно: {success_count}")
    print(f"  - Требуют ручной обработки: {manual_count}")
    print(f"  - Отсутствующие таблицы: {missing_table_count}")
    print(f"  - Ошибки: {failed_count}")
    
    # Сохраняем отчет
    report_path = output_dir / f"{batch_name}_report.json"
    report = {
        'name': batch_name,
        'timestamp': time.time(),
        'input_dir': str(input_dir),
        'output_dir': str(output_dir),
        'success_count': success_count,
        'manual_count': manual_count,
        'missing_table_count': missing_table_count, 
        'failed_count': failed_count,
        'total_count': len(results),
        'elapsed_time': elapsed_time,
        'results': results
    }
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"Отчет сохранен в {report_path}")
    
    # Генерируем HTML/Excel/JSON отчет по итоговому json-отчёту
    if batch_config.get('generate_html_report', True):
        report_generator = ReportGenerator(config)
        reports = report_generator.generate_summary_report(str(report_path))
        if reports:
            print("Сгенерированы итоговые отчеты:")
            for report_type, report_path in reports.items():
                print(f"  - {report_type}: {report_path}")
        else:
            print("Нет данных для итогового отчёта.")
    
    # Считаем процесс успешным, если все скрипты обработаны (даже если требуют ручной обработки или отсутствуют таблицы)
    # Только ошибки считаем неуспехом
    return failed_count == 0

def main():
    """
    Основная функция для запуска пакетной обработки
    """
    parser = argparse.ArgumentParser(description='Пакетная обработка SQL скриптов')
    parser.add_argument('config', help='Путь к YAML-файлу с конфигурацией пакета')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    parser.add_argument('--provider', default='anthropic', help='AI провайдер: anthropic или openai')
    parser.add_argument('--skip-docker-check', action='store_true', help='Не проверять Docker')
    parser.add_argument('--max-iterations', type=int, default=3, help='Максимум итераций AI-конвертации')
    parser.add_argument('--limit', type=int, default=None, help='Максимальное количество файлов для обработки')
    parser.add_argument('--offset', type=int, default=0, help='Пропустить первые N файлов и начать с (N+1)-го')
    
    args = parser.parse_args()
    
    # Проверяем существование файла конфигурации
    config_file = Path(args.config)
    if not config_file.exists() or not config_file.is_file():
        print(f"Ошибка: файл {config_file} не существует")
        return 1
    
    # Запускаем пакетную обработку
    success = process_batch(config_file, args.verbose, args.provider, args.skip_docker_check, args.max_iterations, args.limit, args.offset)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
