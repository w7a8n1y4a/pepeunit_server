# Pepeunit Backend

<div align="center">
    <img align="center" src="https://pepeunit.com/pepeunit-og.jpg"  width="640" height="320">
</div>

Backend Pepeunit REST API, MQTT, Telegram Bot и [DataPipe](https://pepeunit.com/library/docs?doc=deployment/dependencies/datapipe).

Работает на `FastAPI`, `PostgreSQL`, `ClickHouse`, `Redis` и `EMQX`. Команды: `make help`.

Docker-контейнер при запуске исправляет владельца `repo_cache` и затем запускает
приложение от UID/GID `10001:10001` без дополнительных привилегий. При ручном
запуске с `--user` подготовка прав пропускается: смонтированный каталог должен
быть доступен выбранному пользователю. Если файловая система тома не позволяет
менять владельца, подготовьте права на её стороне перед запуском.

## Документация

- [Развёртывание инстанса](https://pepeunit.com/library/docs?doc=deployment/docker/deploy)
- [Backend: зависимость](https://pepeunit.com/library/docs?doc=deployment/dependencies/backend)
- [Переменные окружения Backend](https://pepeunit.com/library/docs?doc=deployment/env-variables/backend)
- [Настройки инстанса](https://pepeunit.com/library/docs?doc=deployment/instance-settings)
- [Сущности](https://pepeunit.com/library/docs?doc=development-pepeunit/mechanics/entities)
- [Тесты](https://pepeunit.com/library/docs?doc=development-pepeunit/tests/module-test)
- [Вклад в проект](https://pepeunit.com/library/docs?doc=development-pepeunit/contribution)
- [Карта репозиториев](https://pepeunit.com/library/docs?doc=development-pepeunit/maps)

## Лицензия

[AGPL-3.0](LICENSE)
