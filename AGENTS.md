# Правила разработки Pepeunit Backend

## Стиль Python

- Пиши имена, комментарии и сообщения об ошибках на английском. Используй
  `snake_case` для модулей, функций и полей, `PascalCase` для классов и
  `UPPER_SNAKE_CASE` для констант.
- Аннотируй параметры и возвращаемые значения. Для коллекций и необязательных
  значений используй `list[T]`, `dict[K, V]` и `T | None`. UUID сущностей
  типизируй как `uuid_pkg.UUID` с импортом `import uuid as uuid_pkg`.
  Текущее время получай через `datetime.now(UTC)`.
- Группируй импорты от стандартной библиотеки к сторонним пакетам и далее к
  абсолютным импортам из `app`. Следуй настройкам Ruff в `pyproject.toml`
  для Python 3.14. Форматтер использует отступ в 4 пробела, двойные кавычки
  и целевую длину строки 79 символов.

## API и бизнес-логика

- Описывай тела запросов и JSON-ответы в `app/schemas/pydantic/` по образцу
  `UnitGroupCreate`, `UnitGroupUpdate` и `UnitGroupRead`. В REST-обработчике
  указывай `response_model` и получай сервис через `Depends(get_*_service)`.
  Для синхронного CRUD используй `def`. Подключай новый роутер
  в `app/routers/v1/endpoints.py`.
- Собирай зависимости сервиса в `ServiceFactory` из `app/configs/rest.py`.
  Держи бизнес-логику в `app/services/`, чтобы её могли вызывать обработчики
  REST, MQTT и Telegram. Для проверки прав агента, владения объектом и его
  видимости используй `self.access_service.authorization` в сервисе.
- Запросы к хранилищам размещай в `app/repositories/`. Для типовых операций
  с SQLModel используй `BaseRepository` и сессию из `get_session`. Фоновым
  задачам открывай собственную сессию через `with BackgroundService(...)`.
- Для списков с пагинацией используй фильтр на базе `BasePaginationRestMixin`
  и функции из `app/repositories/utils.py`. Возвращай в `count` число
  записей после фильтрации, до применения `offset` и `limit`.
- Для пользовательской ошибки поднимай исключение из `app/configs/errors.py`
  с явным кодом, например `raise RepoError(msg, code=ErrorCode.REPO_SYNC_FAILED)`.
  Клиент переводит ошибку по номеру. Добавляя код, выбери свободный номер
  в диапазоне сущности с учётом обоих каталогов. [Коды Python](app/configs/error_codes.py)
  и [коды Go](data_pipe/internal/apperr/codes.go).
- Используй перечисления из `app/dto/enum.py` в общих валидаторах.
  Передавай в `is_valid_object` значение `NotFoundEntity`, в `is_valid_uuid`
  — `UuidEntity`, в `is_valid_json` — `JsonPayload`.

## Хранение данных и DataPipe

- Описывай таблицы PostgreSQL в `app/domain/` как `SQLModel` с `table=True`.
  Новый модуль модели регистрируй в `MODEL_MODULES` в `alembic/env.py`.
  Изменение схемы сопровождай ревизией с `upgrade()` и `downgrade()` в
  `alembic/versions/`. Для ClickHouse добавляй следующий по номеру SQL-файл
  в `clickhouse/migrations/`.
- Параметры окружения читай через `settings` из `app`, настройки инстанса
  и флаги `pu_ff_*` — через `runtime.get()` из того же пакета.
  При добавлении переменной окружения обновляй `.env_example`.
- Обработку сообщений DataPipe размещай в `data_pipe/internal/datapipe/`,
  запросы к хранилищам — в `data_pipe/internal/database/`. Передавай
  `context.Context` в запросы и фоновые циклы.
- При изменении формата конфигурации DataPipe согласуй имена JSON-полей
  и значения перечислений в `app/validators/data_pipe.py` и
  `data_pipe/internal/types/datapipe.go`. Примеры для интеграционных тестов
  лежат в `tests/data/yaml/integra/`.

## Проверки

- `make lint` применяет автоисправления Ruff. Для форматирования изменённых
  файлов используй `uv run ruff format <файлы>` для Python и `gofmt -w <файлы>`
  для Go. Сборку DataPipe проверяй через `make build-data-pipe`.
- Модульные тесты `test_*.py` размещай рядом с кодом в `app/` и запускай
  через `make test-module`.
- Интеграционные сценарии добавляй в `tests/integration/services/`. Используй
  фикстуры из `tests/integration/fixtures/` и функции из `tests/integration/helpers/`.
  Тесты, зависящие от флагов функций, помечай маркерами из `tests/pytest.ini`.
  Запускай `make test-integration` на настроенном тестовом инстансе.
