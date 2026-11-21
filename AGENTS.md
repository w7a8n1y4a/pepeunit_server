# Pepeunit backend development rules

## Python code

- Define application functions only as public methods of the class that owns
  the data or operation. Do not add module-level or nested functions, including
  `async def`. This applies to parsing, validation, and error handling too.
- Private functions, methods, and classes are forbidden. This includes names
  such as `_func`, `__func`, `_Class`, and `__Class`. Use the standard names for
  Python protocol methods such as `__init__`.
- Global variables are forbidden. Keep dependencies and working state on an
  instance or pass them as arguments. This includes module-level parsers and
  hidden module state such as `@cache` or `@lru_cache` on a free function.
- Each method and class must have its own responsibility. Do not add aliases
  for existing calls, classes that only group helpers, or closure factories that
  bind one argument. Keep simple checks next to their data. Express a repeated
  rule in one named method with explicit arguments.
- Keep the main operation readable from top to bottom. Use early returns and
  keep error handling next to the operation that can fail. Avoid nested
  exception handlers and comprehensions that also build errors.
- Comments are forbidden in code and schema files, including YAML.
- Write identifiers and error messages in English. Use `snake_case` for
  modules, methods, and fields, `PascalCase` for classes, and
  `UPPER_SNAKE_CASE` for constants.
- Annotate parameters and return values. Use `list[T]`, `dict[K, V]`, and
  `T | None`. Type entity UUIDs as `uuid_pkg.UUID` with `import uuid as uuid_pkg`.
  Use `datetime.now(UTC)` for the current time.
- Order imports by standard library, third-party packages, then absolute
  imports from `app`. Follow the Python 3.14 Ruff configuration in
  `pyproject.toml`. Use four spaces, double quotes, and a target width of 79.

## Repositories and services

- Write each service operation as a vertical slice of one use case.
  Follow the sequence in `UnitService.create()`: a reader should be able to
  trace the checks, decisions, data changes and result from top to bottom.
  Keep the steps at the same level of detail and include only the steps the
  operation needs. Make the order of side effects visible at the call site.
- Let the service decide when to fetch data or perform an external action.
  Keep queries, file and network access, decoding and transport errors in the
  repository or client that owns them. Keep rules that compare domain values
  with the data they validate. Extract a method when it names a distinct
  responsibility or a shared rule.

- Before adding a method, search for an existing implementation. Read its
  definition and its callers. Check the arguments, return type, and which
  layer calls it. Reuse a matching method directly.
- For file formats, put shared decoding, structural validation, and format
  errors in a base repository under `app/repositories/`. A concrete repository
  selects the model and reads the resources specific to that document.
  `BaseYamlRepository` and `UnitManifestRepository` implement this for YAML.
- Keep schema resource filenames in enums in `app/dto/enum.py`. Store versioned
  dictionaries in `app/schemas/unit_manifest/<contract_version>/dictionary.yml`.
  The manifest contract covers the unit definition, the platform dictionary
  and the rules for applying them together. Both YAML documents declare the
  same top-level `contract_version` string, which also names the directory.
  Use `v<major>` or `v<major>.<minor>` (for example `v1`, `v1.2`, or `v3`).
  Decode the unit YAML once and check its header against
  `ManifestContractVersion` before loading that exact contract's dictionary.
- `GitRepoRepository` selects a file at the requested commit and returns its
  bytes. `UnitManifestService` passes the bytes to its format repository and
  applies manifest rules such as checking search fields against the dictionary.
  Service methods that only forward a repository call are forbidden.
- Keep business logic in `app/services/` so REST, MQTT, and Telegram handlers
  can use it. Assemble service dependencies in `ServiceFactory` in
  `app/configs/rest.py`. Compare a new service's constructor, factory method,
  and validation calls with the closest existing service. For Git operations
  see `RepoService` and `UnitService`. For field validation see
  `app/schemas/pydantic/`.
- Check agent permissions, ownership, and visibility through
  `self.access_service.authorization` in the service.
- Put storage queries in `app/repositories/`. Use `BaseRepository` and a
  session from `get_session` for ordinary SQLModel operations. Open a separate
  session for background work with `with BackgroundService(...)`.
- For pagination use a filter based on `BasePaginationRestMixin` and the
  existing functions in `app/repositories/utils.py`. Return the filtered row
  count before applying `offset` and `limit`.

## API and validation

- Pydantic data models must not declare `model_config` or use `ConfigDict`,
  including `model_config = ConfigDict(...)`. Configure strict document
  parsing where the format repository calls `model_validate`.
- Define request bodies and JSON responses in `app/schemas/pydantic/` following
  `UnitGroupCreate`, `UnitGroupUpdate`, and `UnitGroupRead`. Set
  `response_model` on REST handlers and obtain the service through
  `Depends(get_*_service)`. Use `def` for synchronous CRUD. Register new
  routers in `app/routers/v1/endpoints.py`.
- Raise user-facing errors from `app/configs/errors.py` with an explicit code,
  such as `raise RepoError(msg, code=ErrorCode.REPO_SYNC_FAILED)`. The client
  translates errors by number. Check both catalogs before allocating a number
  in the entity's range. [Python error codes](app/configs/error_codes.py)
  and [Go error codes](data_pipe/internal/apperr/codes.go).
- Use the enums in `app/dto/enum.py` with the shared validators. Pass
  `NotFoundEntity` to `is_valid_object`, `UuidEntity` to `is_valid_uuid`,
  and `JsonPayload` to `is_valid_json`.

## Storage and DataPipe

- Define PostgreSQL tables in `app/domain/` as `SQLModel` with `table=True`.
  Register each new model module in `MODEL_MODULES` in `alembic/env.py`.
  Provide `upgrade()` and `downgrade()` in `alembic/versions/` for schema
  changes. For ClickHouse, add the next numbered SQL file in
  `clickhouse/migrations/`.
- Read environment variables through `settings` from `app`. Read instance
  settings and `pu_ff_*` flags through `runtime.get()` from the same package.
  Update `.env_example` when adding an environment variable.
- Put DataPipe message handling in `data_pipe/internal/datapipe/` and storage
  queries in `data_pipe/internal/database/`. Pass `context.Context` to
  storage calls and background loops.
- Keep DataPipe configuration field names and enum values in sync between
  `app/validators/data_pipe.py` and `data_pipe/internal/types/datapipe.go`.
  Integration examples are in `tests/data/yaml/integra/`.

## Checks

- `make lint` applies Ruff fixes. Format changed Python files with
  `uv run ruff format <files>` and Go files with `gofmt -w <files>`.
  Check the DataPipe build with `make build-data-pipe`.
- Put `test_*.py` module tests next to the code in `app/` and run
  `make test-module`.
- Put integration scenarios in `tests/integration/services/`. Use fixtures
  from `tests/integration/fixtures/` and helpers from `tests/integration/helpers/`.
  Mark feature-dependent tests with the markers defined in `tests/pytest.ini`.
  Run `make test-integration` against a configured test instance.
