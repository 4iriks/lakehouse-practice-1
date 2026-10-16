"""Render the teacher's seven-section report strictly from saved measurements."""
import json
import statistics
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'results'
def read(name): return json.loads((OUT/name).read_text(encoding='utf-8-sig'))
def fmt(value, digits=3): return f'{value:.{digits}f}'
def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join('---' for _ in headers)+'|']+
                     ['| '+' | '.join(map(str,row))+' |' for row in rows])
def mb(size): return fmt(size/1_000_000)
def dur(value):
    import re
    m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m|h)',value)
    if not m: raise ValueError(value)
    return float(m[1])*{'ns':1e-9,'us':1e-6,'ms':.001,'s':1,'m':60,'h':3600}[m[2]]

names=['parquet_gzip','parquet_zstd','orc_snappy','orc_zstd']
labels={'parquet_gzip':'Parquet / GZIP','parquet_zstd':'Parquet / ZSTD',
        'orc_snappy':'ORC / SNAPPY','orc_zstd':'ORC / ZSTD'}
writes=read('writes.json'); trino=read('queries_trino.json')
spark=[]
for name in names:
    for series in range(1,4): spark+=read(f'queries_spark_{name}_{series}.json')
assert len(writes)==12 and len(trino)==48 and len(spark)==48
assert all(x['correct'] for x in trino+spark)
for data in (trino,spark):
    for name in names:
        assert {(x['series'],x['run']) for x in data if x['variant']==name}=={
            (series,run) for series in range(1,4) for run in range(4)}
inv=read('bronze_inventory.json'); layers=read('medallion_inventory.json')
gold=read('gold_q18.json'); quality=read('quality.json'); evolution=read('evolution.json')
small=read('smallfiles_inventory.json'); mq=read('metadata_queries.json')
assert len(mq)==24
checks=read('bronze_checksums.json'); storage=read('storage_audit.json'); spark_bronze=read('spark_bronze.json')
assert len(checks)==15 and all(x['matches'] for x in checks)
env=read('docker_environment.json'); cpu=read('hardware_cpu.json'); system=read('hardware_system.json')
summary=[]
for name in names:
    items=[x for x in inv if x['schema']=='bronze_'+name]
    times=[x['elapsed_s'] for x in writes if x['variant']==name]
    item={'variant':name,'data_bytes':sum(x['active_data_bytes'] for x in items),
          'metadata_bytes':sum(x['metadata_bytes'] for x in items),'files':sum(x['data_files'] for x in items),
          'write_median_s':statistics.median(times),'write_min_s':min(times),'write_max_s':max(times)}
    for engine,data in [('trino',trino),('spark',spark)]:
        for state in ('first','repeat'):
            values=[x['elapsed_s'] for x in data if x['variant']==name and x['state']==state]
            item[f'{engine}_{state}_s']=statistics.median(values)
            item[f'{engine}_{state}_min_s']=min(values); item[f'{engine}_{state}_max_s']=max(values)
    summary.append(item)
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
smallest=min(summary,key=lambda x:x['data_bytes']); fastest_write=min(summary,key=lambda x:x['write_median_s'])
fastest_trino=min(summary,key=lambda x:x['trino_repeat_s']); fastest_spark=min(summary,key=lambda x:x['spark_repeat_s'])

bronze_rows=[]
cols={'customer':'custkey, name','orders':'orderkey, custkey, orderdate, totalprice','lineitem':'orderkey, linenumber, quantity'}
for x in inv:
    if x['schema']=='bronze_parquet_zstd':
        bronze_rows.append([x['table'],cols[x['table']],x['rows'],mb(x['active_data_bytes']),'Parquet','ZSTD','нет',
                            'Открытый столбцовый формат; ZSTD уменьшает объём. Для Q18 нет фильтра по дате, поэтому партиции не добавлены.'])
silver_rows=[]; gold_rows=[]
for x in layers:
    row=[x['table'],'orderkey, total_quantity' if x['table']=='order_quantities' else 'name, custkey, orderkey, orderdate, totalprice, total_quantity',x['rows'],mb(x['active_data_bytes']),'Parquet','ZSTD','нет',
         'Небольшой результат; партиционирование увеличило бы число мелких файлов.']
    (silver_rows if x['schema']=='silver' else gold_rows).append(row)
layer_headers=['Таблица','Колонки','Строк','Вес, МБ','Формат','Сжатие','Партиционирование','Обоснование']
quality_rows=[[k,v['null_rows'],v['duplicate_keys']] for k,v in quality.items() if isinstance(v,dict)]
query_table=table(['Конфигурация','Trino: первый, с','Trino: повторный, с','Spark: первый, с','Spark: повторный, с'],
                  [[labels[x['variant']],fmt(x['trino_first_s']),fmt(x['trino_repeat_s']),fmt(x['spark_first_s']),fmt(x['spark_repeat_s'])] for x in summary])
write_table=table(['Конфигурация','Запись: медиана, с','Диапазон записи, с','Bronze, МБ','Файлов данных','Метаданные Iceberg, МБ'],
                  [[labels[x['variant']],fmt(x['write_median_s']),f"{fmt(x['write_min_s'])}–{fmt(x['write_max_s'])}",mb(x['data_bytes']),x['files'],mb(x['metadata_bytes'])] for x in summary])
cache_table=table(['Конфигурация','Trino: первый / повторный','Spark: первый / повторный'],
                  [[labels[x['variant']],fmt(x['trino_first_s']/x['trino_repeat_s'],2)+'×',fmt(x['spark_first_s']/x['spark_repeat_s'],2)+'×'] for x in summary])
meta_rows=[]
meta_summary={}
for ns,items,target in [('bronze_parquet_zstd',[x for x in inv if x['schema']=='bronze_parquet_zstd'],'128 МиБ'),('bronze_smallfiles',small,'1 МиБ')]:
    qs=[x for x in mq if x['schema']==ns and x['state']=='repeat']
    meta_summary[ns]={'first':statistics.median(x['elapsed_s'] for x in mq if x['schema']==ns and x['state']=='first'),
                      'repeat':statistics.median(x['elapsed_s'] for x in qs),
                      'min':min(x['elapsed_s'] for x in qs),'max':max(x['elapsed_s'] for x in qs)}
    meta_rows.append([target,sum(x['data_files'] for x in items),mb(sum(x['active_data_bytes'] for x in items)),
                      mb(sum(x['metadata_bytes'] for x in items)),
                      fmt(statistics.median(dur(x['query_stats']['planningTime']) for x in qs)),
                      fmt(statistics.median(dur(x['query_stats']['executionTime']) for x in qs)),
                      fmt(statistics.median(x['elapsed_s'] for x in qs))])
footer_table=table(['Конфигурация','Футеры файлов, байт','Внешние метаданные, байт','Сохранённые старые файлы в текущих таблицах, байт'],
                   [[labels[name],sum(x['embedded_footer_bytes'] for x in inv if x['schema']=='bronze_'+name),
                     sum(x['metadata_bytes'] for x in inv if x['schema']=='bronze_'+name),
                     sum(x['retained_data_bytes'] for x in inv if x['schema']=='bronze_'+name)] for name in names])
g=[x for x in summary if x['variant']=='parquet_gzip'][0]; z=[x for x in summary if x['variant']=='parquet_zstd'][0]
sources='[исходные измерения](results), [сводные показатели](results/summary.json)'
base_meta=meta_summary['bronze_parquet_zstd']; small_meta=meta_summary['bronze_smallfiles']
report=f'''# Отчёт по практической работе №1

**Развёртывание локального Lakehouse, ELT-преобразование и бенчмарк форматов и сжатия**

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО | Горбачёв Вадим Александрович |
| Группа | БИВТ-24-сп-1 |
| Вариант | №3 — Q18 «Large Volume Customer» |
| Масштаб | `sf1` |
| Почему выбран такой масштаб | В файле вариантов разрешён самостоятельный выбор масштаба. На компьютере с {system['TotalPhysicalMemory']/2**30:.1f} ГиБ ОЗУ `sf1` позволяет выполнить несколько повторов на двух движках без кластерного оборудования. Материализуются только нужные три таблицы и колонки. Фактический размер Bronze приведён ниже. |

Q18 находит заказы с суммой количества товара больше 300 и возвращает до 100 строк по убыванию стоимости заказа, затем по дате. Результат при `sf1` содержит **{len(gold['rows'])} строк**. `LIMIT 100` задаёт верхнюю границу, а не обязательное число строк. Работа выполнена на `sf1`; 50 ГБ данных не генерировались.

## 2. Поднятые сервисы — и почему именно они

| Сервис | Роль в стеке | Обоснование |
|---|---|---|
| MinIO | S3-совместимое хранилище файлов данных и Iceberg-метаданных | Позволяет локально работать с тем же объектным API, который используют аналитические движки. Не требуется платный облачный аккаунт. |
| PostgreSQL 16.10 | Служебная БД Lakekeeper | Надёжные транзакции для каталога. Аналитические данные TPC-H хранятся в MinIO, а не в PostgreSQL. |
| Lakekeeper 0.9.5 | Iceberg REST Catalog | Даёт Trino и Spark общий каталог таблиц и согласованное состояние снапшотов. |
| Trino 476 | Генерация TPC-H, ELT и SQL-бенчмарк | Встроенный TPC-H-коннектор избавляет от скачивания датасета; Iceberg-коннектор читает и пишет Parquet и ORC. |
| Spark 3.5.6 + Iceberg 1.9.2 | Второй движок и инструмент из пункта 6.2 | Проверяет доступность одних и тех же таблиц из другого движка и позволяет сравнить выполнение Q18. |

Аппаратная среда: **{cpu['Name'].strip()}**, {cpu['NumberOfCores']} физических ядер / {cpu['NumberOfLogicalProcessors']} логических процессоров. Docker {env['ServerVersion']}, Linux-контейнеры в WSL2, доступно {env['MemTotal']/2**30:.2f} ГиБ памяти. Trino и Spark ограничены четырьмя CPU и 4 ГиБ памяти контейнера; Java heap каждого — 3 ГиБ. Во время Spark-бенчмарка Trino остановлен. Полные параметры: [config](config), подтверждение сервисов и версий — [results](results).

### Скрипты запуска

```powershell
python -m venv .venv
.\\.venv\\Scripts\\python.exe -m pip install -r requirements-lock.txt
.\\.venv\\Scripts\\python.exe scripts/prepare_source.py  # официальный архив MinIO и SHA256
docker compose build minio                           # сборка закреплённого релиза
docker compose up -d --wait postgres minio lakekeeper
.\\.venv\\Scripts\\python.exe scripts/lab.py setup     # бакет и Iceberg warehouse
docker compose up -d --wait trino                      # SQL-движок
.\\run.ps1 -Stage Load                                # Bronze, Silver и Gold
```

Полная конфигурация — [docker-compose.yml](docker-compose.yml), последовательный запуск всех этапов — [run.ps1](run.ps1). MinIO собран из закреплённого официального исходного релиза `RELEASE.2025-09-07T16-13-09Z` с проверкой SHA256. Сервисы публикуют порты только на localhost.

## 3. Разбор запроса — какие данные нужны

| Таблица TPC-H | Нужные колонки | Использование |
|---|---|---|
| customer | custkey, name | Соединение клиента с заказом; ключ и имя в результате. |
| orders | orderkey, custkey, orderdate, totalprice | Соединения, группировка и вывод; сортировка по стоимости и дате. |
| lineitem | orderkey, quantity | Сумма количества по заказу, условие `sum(quantity) > 300`, итоговый агрегат. |
| lineitem: контрольный ключ | linenumber | Дополнительно сохранён для проверки уникальности позиции `(orderkey, linenumber)`; сам Q18 его не читает. |

Полный запрос: [sql/q18.sql](sql/q18.sql). Имена колонок соответствуют коннектору Trino TPC-H, без префиксов `c_`, `o_`, `l_`.

## 4. Медальон — решения по каждому слою

### Bronze — исходные строки

Основной физический слой — `lake.bronze_parquet_zstd`. Представления `lake.bronze.*` ссылаются на него и не создают дополнительных файлов. Форматы для бенчмарка содержат те же строки и колонки.

Для всех 12 физических Bronze-таблиц основного сравнения и трёх таблиц опыта с мелкими файлами проверены количество строк и независимая от порядка контрольная сумма всех сохранённых колонок относительно `tpch.sf1`. Все проверки совпали; фактические типы и контрольные суммы находятся в [results/bronze_checksums.json](results/bronze_checksums.json).

{table(layer_headers,bronze_rows)}

### Silver — проверка, нормализация и обогащение

Проверены пропуски обязательных колонок и уникальность ключей:

{table(['Таблица','Строк с пропусками','Дублирующихся ключей'],quality_rows)}

Заказов без клиента: **{quality['orphan_orders']}**. Позиций без заказа: **{quality['orphan_lineitems']}**. Проверки синтетического TPC-H не выявили строк, требующих удаления. В Silver имена обработаны `trim`, денежное значение приведено к `decimal(15,2)`, сумма количества — к `decimal(18,2)`. Обогащение выполнено соединением клиента, заказа и агрегированного количества.

{table(layer_headers,silver_rows)}

### Gold — витрина Q18

{table(layer_headers,gold_rows)}

Gold совпал с результатом исходного Q18 на `tpch.sf1`. Полная витрина: [results/gold_q18.csv](results/gold_q18.csv). Небольшие Silver и Gold не партиционируются: для этого запроса разбиение по дате не даёт отсечения данных и создаёт лишние файлы.

## 5. Тесты производительности

### Методика

Данные записывал Trino. Для каждой конфигурации выполнены три полные записи трёх таблиц; порядок конфигураций менялся между повторами. Время записи включает генерацию TPC-H, сериализацию, сжатие и commit Iceberg. Размеры измерены после третьей записи. Коды сжатия проверены чтением метаданных физических Parquet/ORC-файлов.

В каждом движке для каждой конфигурации выполнены три серии по четыре запроса: первый после запуска нового процесса движка и три повторных. Итого **48 измерений Trino и 48 Spark**. Время измеряется от отправки SQL до получения всех строк, без запуска JVM/контейнера и без последующего сбора статистики. Каждый результат сравнивается с эталоном. В таблицах указаны медианы: три значения для первого запроса и девять для повторного. Все 96 проверок результата прошли.

Запрашивался исходный Q18 над Bronze, а не маленькая Gold-витрина. Во всех записях использованы один writer, одинаковая проекция и целевой размер файла 128 МиБ. Партиционирование отсутствует. Для Spark — `local[4]`, восемь shuffle-разделов, AQE включён. Данные не помещались в `CACHE TABLE`.

### Обязательное сравнение GZIP и ZSTD на Bronze

{table(['Сжатие на Bronze (Parquet)','Время повторного Q18 в Trino, с','Размер Bronze, МБ','Вывод'],[
['GZIP',fmt(g['trino_repeat_s']),mb(g['data_bytes']),f"Размер относительно ZSTD: {g['data_bytes']/z['data_bytes']:.3f}×; медиана записи {fmt(g['write_median_s'])} с."],
['ZSTD',fmt(z['trino_repeat_s']),mb(z['data_bytes']),f"Отношение ZSTD/GZIP: размер {z['data_bytes']/g['data_bytes']:.3f}×, время повторного Q18 {z['trino_repeat_s']/g['trino_repeat_s']:.3f}×. Выбран для основного медальона по сочетанию этих показателей."]])}

### Полное сравнение форматов и кодеков

{write_table}

{query_table}

Самый маленький Bronze получился у **{labels[smallest['variant']]}**: {mb(smallest['data_bytes'])} МБ. Минимальная медиана записи — **{labels[fastest_write['variant']]}**, {fmt(fastest_write['write_median_s'])} с. Для повторного Q18 минимальная медиана в Trino у **{labels[fastest_trino['variant']]}**, в Spark — у **{labels[fastest_spark['variant']]}**. Это наблюдение для данного запуска, объёма и набора колонок; оно не устанавливает универсального победителя.

### Кэширование и прогрев

{cache_table}

Коэффициент равен медиане первого запуска, делённой на медиану повторного. Перезапуск очищает состояние процесса движка, но кэш файловой системы и MinIO остаётся. На разницу также влияют JIT-компиляция, подготовка плана и загрузка классов. Поэтому первый запуск обозначен как «первый после перезапуска», а не как полностью холодный. Метаданные Iceberg кэшируются Trino; отдельный файловый кэш Trino не включён. По этим измерениям нельзя приписать весь выигрыш только кэшированию.

### Объём метаданных и количество файлов

{footer_table}

Для Parquet здесь учтены footer и завершающие восемь байт файла, для ORC — footer, postscript и stripe statistics; внутренние индексы каждого stripe отдельно не подсчитывались. Эти байты уже включены в размер файлов данных. Внешние метаданные Iceberg — отдельные объекты в каталоге `metadata/`. Сохранённые файлы данных старых снапшотов подсчитаны отдельно в пределах текущих таблиц. Объекты удалённых пробных таблиц не включены в размер действующего Bronze.

Итоговый аудит всего бакета: {mb(storage['total_bytes'])} МБ, из них активные данные всех таблиц — {mb(storage['active_data_bytes'])} МБ, сохранённые старые файлы действующих таблиц — {mb(storage['retained_data_bytes_current_tables'])} МБ, их внешние метаданные — {mb(storage['metadata_bytes_current_tables'])} МБ, остальные объекты, включая удалённые пробные таблицы, — {mb(storage['other_bytes_including_dropped_trials'])} МБ. Подробности: [results/storage_audit.json](results/storage_audit.json).

Контрольный опыт выполнен только для Parquet/ZSTD: те же строки, запрос и параметры writer, меняется целевой размер файла.

{table(['Целевой размер','Файлов данных','Данные, МБ','Метаданные, МБ','Планирование, с','Выполнение, с','Полное время, с'],meta_rows)}

Показаны медианы девяти повторных запросов для каждого размещения. Планирование и выполнение — поля `planningTime` и `executionTime` статистики Trino; полное время измерено клиентом. Это разные измерители, их медианы не обязаны складываться в полное время.

Медиана первого Q18 составила {fmt(base_meta['first'])} с для исходного размещения и {fmt(small_meta['first'])} с для мелких файлов. Диапазоны повторных запусков: {fmt(base_meta['min'])}–{fmt(base_meta['max'])} с и {fmt(small_meta['min'])}–{fmt(small_meta['max'])} с соответственно. Они показывают разброс, который нужно учитывать вместе с разницей медиан.

Целевой размер — настройка writer, а не гарантия точного размера каждого объекта. Сопоставляется фактическое число файлов. Этот опыт оценивает совместное влияние файловой раскладки и метаданных; число файлов меняет и объём работы по открытию/чтению объектов. Однозначно выделить только стоимость метаданных из общего времени нельзя. На маленьких Silver/Gold служебные файлы могут занимать заметную долю хранения даже при малом числе строк.

Исходные данные для всех расчётов: {sources}. Время и размеры не экстраполируются на 50 ГБ. Стоимость обсуждается через объём хранения, количество объектов и время вычислений; денежные тарифы облака в локальном опыте отсутствуют.

## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Ответ |
|---|---|
| Зачем нужны большие данные и lakehouse, а не обычная БД? | Lakehouse позволяет хранить большие наборы файлов в объектном хранилище и подключать разные вычислительные движки. Iceberg добавляет схемы, атомарные изменения и снапшоты. Для маленькой транзакционной задачи обычная БД проще; здесь смысл стенда — проверить разделение хранения и вычислений на практике. |
| Пример schema evolution на Silver | В `silver.evolution_demo` добавлена `processing_note varchar`, затем переименована в `etl_note`. Список файлов данных остался тем же, число строк не изменилось; старая информация доступна после изменения схемы. Подтверждение: `results/evolution.json`. |
| Пример восстановления из снапшота | Сохранён snapshot `{evolution['snapshot_id']}`. После удаления одного заказа число строк изменилось с {evolution['count_before']} до {evolution['count_after_delete']}. Чтение `FOR VERSION AS OF` вернуло прежние данные, а `rollback_to_snapshot` восстановил {evolution['count_after_restore']} строк. Сверены все поля восстановленного набора. |

Пример команд из выполненного опыта:

```sql
ALTER TABLE lake.silver.evolution_demo ADD COLUMN processing_note varchar;
ALTER TABLE lake.silver.evolution_demo RENAME COLUMN processing_note TO etl_note;

SELECT * FROM lake.silver.evolution_demo
FOR VERSION AS OF {evolution['snapshot_id']};

ALTER TABLE lake.silver.evolution_demo
EXECUTE rollback_to_snapshot(snapshot_id => {evolution['snapshot_id']});
```

Номер снапшота относится к сохранённому запуску; при новом выполнении скрипт получает новый ID из `$snapshots`. Откат снапшота восстанавливает данные; изменения схемы проверяются отдельно. Исходные рабочие Silver и Gold в демонстрации не изменялись.

### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбран |
|---|---|
| Apache Spark | Через Iceberg REST Catalog читает те же Bronze-таблицы, что Trino. Закрывает требование второго инструмента и позволяет независимо проверить Q18 без отдельной копии данных или установки BI-системы. |

Развёртывание задано сервисом `spark` в Compose; подключение к Lakekeeper и MinIO — в [config/spark-defaults.conf](config/spark-defaults.conf).

Spark запускается как отдельное приложение в контейнере для каждой серии и конфигурации. Снимок `docker compose --profile spark ps -a` во время работы: [results/services_during_spark.txt](results/services_during_spark.txt); он показывает активный Spark и остановленный Trino.

```powershell
docker compose stop trino
docker compose --profile spark run --rm spark /lab/scripts/spark_benchmark.py --variant parquet_zstd --series 1
docker compose up -d --wait trino
```

Внутри Spark выполнены Q18 с `collect()`, затем `spark.table("lake.bronze_parquet_zstd.customer")`, аналогичные чтения `orders` и `lineitem`, `count()` и вывод первых пяти строк. Реальные схемы, количества и примеры строк сохранены в [results/spark_bronze.json](results/spark_bronze.json). Полные измерения — `results/queries_spark_*.json`.

Фактический результат чтения Bronze в Spark:

{table(['Таблица','Строк','Пример первой прочитанной строки'],[[name,item['count'],'`'+json.dumps(item['sample'][0],ensure_ascii=False)+'`'] for name,item in spark_bronze.items()])}

## 7. Выводы

Медальон разделил исходные строки, подготовку крупных заказов и готовую аналитическую витрину, причём Gold совпал с исходным Q18. Самое компактное хранение в этом опыте обеспечил {labels[smallest['variant']]}, а минимальное время записи — {labels[fastest_write['variant']]}. Минимальные медианы повторного Q18 получены для {labels[fastest_trino['variant']]} в Trino и {labels[fastest_spark['variant']]} в Spark, поэтому выбор формата зависит и от движка. В опыте с целевым размером 1 МиБ получилось {sum(x['data_files'] for x in small)} файлов, медиана первого запроса составила {fmt(small_meta['first'])} с против {fmt(base_meta['first'])} с у исходного размещения, а диапазоны повторных запусков перекрывались. Для выбранной задачи Parquet/ZSTD сочетает компактное хранение и совместимость с двумя движками; решение для другого объёма следует проверять отдельным бенчмарком.
'''
(ROOT/'отчет.md').write_text(report,encoding='utf-8')
print('Report written from',len(writes),'writes and',len(trino)+len(spark),'query measurements')
