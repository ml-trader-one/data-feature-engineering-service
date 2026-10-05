FROM python:3.11-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем Poetry для установки зависимостей из lock-файла
RUN pip install --no-cache-dir poetry==2.5.1
ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# Копируем pyproject.toml
COPY pyproject.toml poetry.lock /app/

# Устанавливаем зависимости из poetry.lock
RUN poetry sync --only main --no-root --no-interaction

# Копируем исходный код
COPY ./app /app/app

# Запускаем скрипт-воркер (не веб-сервер)
CMD ["python", "-m", "app.main"]
