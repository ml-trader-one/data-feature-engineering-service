# data-feature-engineering-service

Python-сервис проекта ML Trader. Поддерживаемое окружение: Python 3.11–3.12 и Poetry 2.5.1.

## Установка

```bash
poetry env use python3.11
poetry sync --with dev
poetry run pre-commit install
```

Poetry создаёт `.venv` в каталоге проекта. Зависимости фиксируются в `poetry.lock`;
`pyproject.toml`, `poetry.lock` и `poetry.toml` хранятся в Git, а `.venv` исключена.
После клонирования команда `poetry sync --with dev` восстанавливает окружение.
Хуки устанавливаются отдельно в каждой локальной копии репозитория.

## Конфигурация и запуск

```bash
cp .env.example .env
poetry run serve
```

Параметры окружения описаны в `.env.example` и `app/config.py`. Для рабочего запуска
нужны адреса PostgreSQL, Kafka или MLflow согласно назначению сервиса; для ingestion
также нужен действующий `INVEST_TOKEN`. Секреты хранятся в локальном `.env`.

## Проверки

```bash
poetry check --lock
poetry run pre-commit run --all-files
poetry run pytest -q
```

Ruff проверяет ошибки Python, импорты и современный синтаксис, а также форматирование.
Для исправления форматирования:

```bash
poetry run ruff check . --fix
poetry run ruff format .
```

## Контейнер

```bash
docker build -t data-feature-engineering-service .
```

Контейнер устанавливает рабочие зависимости из `poetry.lock`. В GitHub Actions
проверяются lock-файл, pre-commit, тесты, сборка образа и импорт приложения.
Проверка образа не запускает сервис и не публикует контейнер.
