import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные из .env файла
env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=env_path)

# Пути к директориям
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
CONVERTED_DIR = BASE_DIR / "converted"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

# Убедимся, что все директории существуют
for dir_path in [SCRIPTS_DIR, CONVERTED_DIR, LOGS_DIR, REPORTS_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# Настройки PostgreSQL из .env или значения по умолчанию
PG_CONFIG = {
    "host": os.getenv('PG_HOST', 'localhost'),
    "port": int(os.getenv('PG_PORT', 5432)),
    "database": os.getenv('PG_DATABASE', 'testdb'),
    "user": os.getenv('PG_USER', 'testuser'),
    "password": os.getenv('PG_PASSWORD', 'testpassword')
}

# Строка подключения для psql
PG_CONNECTION_STRING = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"

# Сопоставление типов данных
DATA_TYPE_MAPPING = {
    "NVARCHAR": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "NCHAR": "CHAR",
    "CHAR": "CHAR",
    "DATETIME": "TIMESTAMP",
    "DATETIME2": "TIMESTAMP",
    "SMALLDATETIME": "TIMESTAMP",
    "DATE": "DATE",
    "TIME": "TIME",
    "DECIMAL": "NUMERIC",
    "NUMERIC": "NUMERIC",
    "MONEY": "NUMERIC(19,4)",
    "SMALLMONEY": "NUMERIC(10,4)",
    "FLOAT": "FLOAT8",
    "REAL": "FLOAT4",
    "BIGINT": "BIGINT",
    "INT": "INTEGER",
    "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "BIT": "BOOLEAN",
    "UNIQUEIDENTIFIER": "UUID",
    "VARBINARY": "BYTEA",
    "BINARY": "BYTEA",
    "IMAGE": "BYTEA",
    "TEXT": "TEXT",
    "NTEXT": "TEXT",
    "XML": "XML",
}

# Соответствие функций
FUNCTION_MAPPING = {
    "GETDATE()": "CURRENT_TIMESTAMP",
    "GETUTCDATE()": "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
    "SYSDATETIME()": "CURRENT_TIMESTAMP",
    "SYSUTCDATETIME()": "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
    "ISNULL": "COALESCE",
    "CONVERT": "CAST",
    "DATEADD": "date + interval",
    "DATEDIFF": "EXTRACT(EPOCH FROM (date2 - date1))/86400",
    "DATEPART": "EXTRACT",
    "YEAR": "EXTRACT(YEAR FROM ",
    "MONTH": "EXTRACT(MONTH FROM ",
    "DAY": "EXTRACT(DAY FROM ",
    "GETANSINULL": "current_setting('DateStyle')",
    "UPPER": "UPPER",
    "LOWER": "LOWER",
    "LEN": "LENGTH",
    "SUBSTRING": "SUBSTRING",
    "LEFT": "LEFT",
    "RIGHT": "RIGHT",
    "RTRIM": "RTRIM",
    "LTRIM": "LTRIM",
    "REPLACE": "REPLACE",
    "STUFF": "OVERLAY",
    "CHARINDEX": "POSITION",
    "PATINDEX": "POSITION",
    "@@IDENTITY": "lastval()",
    "@@ROWCOUNT": "ROW_COUNT",
    "@@ERROR": "SQLSTATE",
    "@@SERVERNAME": "current_setting('server_version')",
    "@@VERSION": "version()",
    "@@SPID": "pg_backend_pid()",
}

# Параметры по умолчанию для тестирования
DEFAULT_PARAMS = {
    'ACTIVESTATUS': 10,
    "date": "'2023-01-01'::timestamp",
    "id": "1",
    "name": "'test_name'",
    "value": "100",
    "text": "'sample_text'",
    "startDate": "'2023-01-01'::timestamp",
    "endDate": "'2023-12-31'::timestamp",
    "userId": "1",
    "customerId": "1000",
    "productId": "500",
    "orderId": "12345",
    "status": "'active'",
    "category": "'general'",
    "code": "'ABC123'",
    "price": "99.99",
    "quantity": "10",
    "flag": "true",
    "description": "'This is a default description'",
}

# Максимальное время выполнения скрипта в секундах
MAX_EXECUTION_TIME = int(os.getenv('MAX_EXECUTION_TIME', 30))

# Настройки использования нейросетей
USE_AI_CONVERSION = os.getenv('USE_AI_CONVERSION', 'true').lower() == 'true'

# Провайдер нейросети ('openai' или 'anthropic')
AI_PROVIDER = os.getenv('AI_PROVIDER', 'openai')

# Настройки OpenAI
# или 'gpt-3.5-turbo' для более быстрой работы
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4')

# Настройки Anthropic
ANTHROPIC_MODEL = os.getenv('ANTHROPIC_MODEL', 'claude-3-sonnet-20240229')

# Общие настройки для нейросетей
# Низкая температура для более предсказуемых результатов
AI_TEMPERATURE = float(os.getenv('AI_TEMPERATURE', 1))
# Максимальная длина ответа 
AI_MAX_TOKENS = int(os.getenv('AI_MAX_TOKENS', 32000))
# Количество попыток использования нейросети при ошибках
AI_RETRY_COUNT = int(os.getenv('AI_RETRY_COUNT', 2))

# Порог использования нейросети
# Если стандартные методы не справляются после этого количества попыток, 
# будет использована нейросеть
AI_FALLBACK_THRESHOLD = int(os.getenv('AI_FALLBACK_THRESHOLD', 2))

# Настройка использования реальной БД для тестирования скриптов
# Если False, то будет использоваться только синтаксическая проверка
USE_REAL_DB_TESTING = os.getenv('USE_REAL_DB_TESTING', 'true').lower() == 'true'

# Включить улучшенный парсер для анализа контекста параметров
USE_IMPROVED_PARSER = True
