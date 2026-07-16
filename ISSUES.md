# Критические проблемы: `cabbagok/amqp.py`

Анализ от 2026-05-31.

## 1. Race condition в `_on_request` / `_tasks` (L355-361)
```python
self._tasks.add(task)
task.add_done_callback(lambda fut: self._tasks.remove(fut))
```
Если задача удаляется дважды или отсутствует в множестве, `set.remove()` бросит `KeyError`.
**Фикс:** использовать `self._tasks.discard(fut)` вместо `remove`.

## 2. `Set changed size during iteration` в `stop()` (L446-447)
```python
for consumer_tag in self._subscriptions:
    await self.unsubscribe(consumer_tag)
```
`await` внутри цикла по `set`; при потере соединения `run_server` вызывает `self._subscriptions.clear()` → `RuntimeError`.
**Фикс:** итерировать по копии — `list(self._subscriptions)`.

## 3. Потеря `callback_queue` при реконнекте в клиентском режиме (L488)
`send_rpc` вычисляет `properties` с `reply_to=self.callback_queue` (L487-490) ДО публикации. При реконнекте внутри `_publish_with_retry` → `connect()` создаётся новая эксклюзивная очередь. Ответ придёт на старую (удалённую) очередь → таймаут `ServiceUnavailableError`.

## 4. Бесконечный цикл в `wait_connected` (L545-548)
Нет таймаута и проверки `keep_running`. Если соединение никогда не установится (например `run_server` упал), цикл крутится вечно.

## 5. Проглоченные исключения в фоновом `run_server` (L440)
```python
asyncio.ensure_future(self.run_server())
```
Задача не сохраняется и не отслеживается. Необработанное исключение теряется ("Task exception was never retrieved"), приложение зависает в `wait_connected`.

## 6. `nack` без проверки `channel.is_open` в `handle_rpc` (L386)
```python
await channel.basic_client_nack(delivery_tag=envelope.delivery_tag)
```
В отличие от `basic_client_ack` (L407, защищён `if channel.is_open`), `nack` в `except` не проверяет состояние канала → исключение из фоновой задачи.

## 7. Ошибка кодирования ответа при несоответствии `raw` (L395)
```python
payload=response if self.raw else response.encode("utf-8"),
```
Если `raw=False`, но хендлер вернул `bytes` (или наоборот) — `AttributeError`/`TypeError` после успешного выполнения хендлера. Сообщение не будет ни ack, ни nack → зависнет неподтверждённым.

## 8. Deprecated `asyncio.get_event_loop()` (L46, L516, L587)
Устарело в Python 3.10+, выбрасывает `DeprecationWarning`/ошибки вне работающего цикла. Особенно опасно в `__init__` (L46).

## 9. Коллизия `correlation_id` перетирает Future (L516)
```python
self._responses[correlation_id] = asyncio.get_event_loop().create_future()
```
При пользовательском `correlation_id`, совпадающем с уже ожидающим запросом, прежний Future теряется → первый вызывающий зависнет до таймаута.

## 10. Мутация общего списка `start_subscriptions` (L152, L339)
`self.start_subscriptions = subscriptions or []` — при передаче внешнего списка `subscribe(add_to_start=True)` мутирует объект вызывающего. Сравнение `params not in ...` зависит от идентичности функций-хендлеров.

---

## Сводка приоритетов

| # | Проблема | Серьёзность |
|---|----------|-------------|
| 2 | `Set changed size during iteration` в `stop()` | Высокая — гарантированный краш |
| 3 | Потеря callback_queue при реконнекте | Высокая — молчаливый таймаут |
| 1 | `KeyError` в `_tasks.remove` | Высокая |
| 4, 5 | Зависание `wait_connected` / потеря исключений | Высокая |
| 6, 7 | Необработанные исключения в `handle_rpc` | Средняя |
| 8, 9, 10 | Deprecated loop / коллизии / мутация | Средняя-низкая |
