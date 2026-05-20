# Race Events RabbitMQ Pipeline

Демонстрация RabbitMQ с примером гоночных событий (F1, NASCAR, LeMans).

## Архитектура

- Producer (FastAPI): Генерирует события о гонках и публикует их в direct exchange `race_events`
- RabbitMQ Exchange: `race_events` с routing keys `f1`, `nascar`, `lemans`
- Consumer 1: Читает все события и выводит обновления в реальном времени
- Consumer 2: Читает все события и отслеживает лидеров
- RabbitMQ Management UI: мониторинг очередей и сообщений

## Запуск

1. Поднять стек:

```bash
docker compose up -d --build
```

2. Проверить контейнеры:

```bash
docker compose ps
```

3. Генерация событий запускается автоматически при старте приложения. Откройте Swagger: http://localhost:8001/docs
   или используйте ручные вызовы для управления генератором (например, остановка):

```bash
# Остановить генерацию
curl -X POST "http://localhost:8001/stop_race_events" -H "Content-Type: application/json"

# Запустить вручную (необязательно, если уже запущено автоматически)
curl -X POST "http://localhost:8001/start_race_events" -H "Content-Type: application/json"
```

4. Смотреть логи:

```bash
docker compose logs -f consumer1
```

```bash
docker compose logs -f consumer2
```

5. RabbitMQ UI:

- URL: http://localhost:15672
- Login: guest
- Password: guest

## Структура сообщения

```json
{
  "race": "F1",
  "driver": "Hamilton",
  "position": 1,
  "lap": 50,
  "timestamp": "2026-05-20T11:20:16.123456"
}
```

## API Endpoints

- GET / - статус приложения
- POST /start_race_events - начать генерацию
- POST /stop_race_events - остановить генерацию
- GET /race_status - текущий статус гонок
- GET /docs - Swagger UI

## Остановка

```bash
docker compose down
```
