# DBEngine
DBEngine &mdash; клиент-серверное на межпроцессном уровне приложение для хранения коллекций данных.

Коллекция данных однозначно идентифицируется тремя строковыми параметрами:
* названием пула, хранящим схемы данных;
* названием схемы, хранящей коллекции;
* названием коллекции.

Каждый уровень представляет собой ассоциативный контейнер вида B-tree с кастомизируемым аллокатором, на самом глубоком уровне по строковому ключу хранятся структуры вида { uint64_t, string }, заданные статически.

Приложение работает в 2 режимах:
* in-memory: все данные хранятся в ОЗУ;
* гибрид: данные вынесены в файловую систему, в коллекциях вместо структур хранятся файловые метки типа long.

### Сервер

Серверная часть представляет собой кластер процессов, взаимодействующих через очереди сообщений System V. Кластер состоит из сервера-менеджера, являющегося точкой входа, нескольких серверов-хранилищ и логирующего сервера для централизованного сбора логов.

Количество серверов-хранилищ может изменяться динамически. Каждый сервер-хранилище хранит некоторый непрерывный интервал из области значений поля-ключа, раз в N операций с коллекциями, сервер-менеджер проверяет равномерность распределения данных и, если необходимо, перераспределеяет их.

Строковые данные на серверах-хранилищах хранятся с использованием паттерна "приспособленец", доступ к строковому пулу реализован через паттерн "одиночка".

Логирующий сервер настраивается с помощью конфигурационного файла формата json, в котором задаются формат, а также комбинации уровней важности и путей логирования:
```json
{
    "format_string": "%d %t - [%s] %m",
    "logger_files":
    {
        "./some_file": [ "DEBUG" ],
        "../folder/another_file": [ "TRACE" ],
        "console": [ "DEBUG", "INFORMATION" ]
    }
}
```

Запуск: \<program\> \<-f/-m\> \<configFilePath\> \<loggerConfigPath\>
* -f/-m &mdash; режимы запуска, гибрид и in-memory соответственно
* config_file_path &mdash; путь к конфигурационному файлу формата JSON
* logger_config_path &mdash; путь вида "token:token:token" к конфигурации логгера в рамках конфигурационного файла

Команды сервера:
* shutdown &mdash; отключение серверного кластера;
* run &mdash; запуск дополнительного storage-сервера;
* terminate &mdash; отключение последнего добавленного storage-сервера.

### Клиент

Команды клиента:
* addPoolpoolName \<BTreeParam\> &mdash; добавление пула с заданным параметром B-дерева;
* addSchema \<poolName\> \<schemaName\> \<BTreeParam\> &mdash; добавление схемы с заданным параметром B-дерева;
* addCollection \<poolName\> \<schemaName\> \<collectionName\> \<BTreeParameter\> \<allocatorType\> \<fitMode\> &mdash; добавление коллекции с заданным
параметром B-дерева, аллокатором (globalHeap, boundaryTags, sortedList, buddiesSystem, redBlackTree) и его режимом выделения (first, best, worst);
* disposePool \<poolName\> &mdash; удаление пула;
* disposeSchema \<poolName\> \<schemaName\> &mdash; удаление схемы;
* disposeCollection \<poolName\> \<schemaName\> \<collectionName\> &mdash; удаление коллекции;
* add \<poolName\> \<schemaName\> \<collectionName\> \<stringKey\> \<uint64Value\> \<stringValue\> &mdash; добавление записи в коллекцию, для имени с пробелами, нужно
обернуть строку в одинарные кавычки;
* update \<poolName\> \<schemaName\> \<collectionName\> \<stringKey\> \<uint64Value\> \<stringValue\> &mdash; обновлении записи в коллекции;
* dispose \<poolName\> \<schemaName\> \<collectionName\> \<stringKey\> &mdash; удаление записи из коллекции;
* obtain \<poolName\> \<schemaName\> \<collectionName\> \<stringKey\> &mdash; чтение записи из коллекции;
* obtain \<poolName\> \<schemaName\> \<collectionName\> \<stringKeyLeft\> \<stringKeyRight\> &mdash; чтение диапазона записей из коллекции;
* reconnect &mdash; переподключение к серверу;
* executeFile \<fileName\>
 