Сервис читает сообщения из очереди, и каждое отправляет по REST в сервис Loader.
Те сообщения, которые не удалось отправить в Loader, сохраняются в Redis.