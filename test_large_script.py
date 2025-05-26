import sys
sys.path.append('.')
from src.ai_converter import AIConverter
from types import SimpleNamespace

# Создаем конфигурацию
config = SimpleNamespace()
config.LARGE_SCRIPT_THRESHOLD = 500  # Скрипты больше 500 строк считаются большими
config.LARGE_SCRIPT_CHUNK_SIZE = 200  # Размер чанка для разбиения больших скриптов

# Инициализируем конвертер
ai = AIConverter(config)

# Загружаем скрипт для тестирования
with open('scripts/examples/example11.sql', 'r') as f:
    script = f.read()

# Проверяем, считается ли скрипт большим
is_large = ai.is_large_script(script)
print('\nЭто большой скрипт?', is_large)

# Разбиваем скрипт на части
lines = script.splitlines()
chunk_size = config.LARGE_SCRIPT_CHUNK_SIZE
chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
print(f'Скрипт разделен на {len(chunks)} частей по ~{chunk_size} строк каждая')

# Выводим информацию о первой и последней частях
print(f'\nПервая часть (строки 1-{min(chunk_size, len(lines))}):\n')
print('\n'.join(chunks[0][:5]) + '...')

if len(chunks) > 1:
    last_chunk_start = (len(chunks) - 1) * chunk_size + 1
    last_chunk_end = min(last_chunk_start + len(chunks[-1]) - 1, len(lines))
    print(f'\nПоследняя часть (строки {last_chunk_start}-{last_chunk_end}):\n')
    print('...' + '\n'.join(chunks[-1][-5:]))

print(f'\nОбщее количество строк в скрипте: {len(lines)}')