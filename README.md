# ETL для парсинга курса валют с сайта цетробанка

Парсит курсы валют. Есть etl_id уникальный номер(прокидывается в бд в лог и саму таблицу с курсами), retry, логирование, выгрузка в csv данных.


### Запуск:


```
git clone https://github.com/AspidovNikita/etl.git
```

```
cd etl
```


```
python -m pip install --upgrade pip
```

```
pip install -r requirements.txt
```



Запуск

```
python centrobank.py
```
crontab для расписания.


### Конфигурация в файле
```



api:
  user_agent: "Mozilla/5.0 (compatible; CurrencyParser/1.0)"
  cbr_url: "http://www.cbr.ru/scripts/XML_daily.asp"

db:
  name: "etl"
  user: "etl"
  password: "etl"
  host: "localhost"
  port: 5432

parser:
  db_enabled: true (либо в бд выбираем либо в csv)
  no_csv: false
  output_file: "currency_rates.csv"

csv_log:
  log_file: "etl_runs.csv"
  no_log: false
```
