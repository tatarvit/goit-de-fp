# Final Project - Data Engineering

Цей фінальний проєкт складається з двох частин:

End-to-End Streaming Pipeline
End-to-End Batch Data Lake

Проєкт демонструє побудову повного пайплайну обробки даних у streaming та batch режимах з використанням сучасних інструментів Data Engineering.

## Частина 1. End-to-End Streaming Pipeline

У першій частині реалізовано стримінговий пайплайн для обробки спортивних результатів атлетів.

### Джерела даних
* MySQL таблиця: olympic_dataset.athlete_bio (біологічні дані атлетів)
* Kafka topic: athlete_event_results (результати змагань)

### Основні етапи
* Зчитування даних з MySQL за допомогою Spark
* Фільтрація некоректних значень (height, weight)
* Зчитування даних з Kafka
* Перетворення JSON у DataFrame
* Join даних за athlete_id
* Розрахунок середніх значень для:
- sport
- medal
- sex
- country_noc
* Запис результатів у:
- Kafka topic
- базу даних
- Результат

### Фінальний стримінговий DataFrame містить:
* sport
* medal
* sex
* country_noc
* avg_weight
* avg_height
* timestamp

## Частина 2. End-to-End Batch Data Lake

У другій частині реалізовано batch pipeline з використанням multi-hop Data Lake архітектури.

## Джерела даних

Дані завантажуються з FTP:

https://ftp.goit.study/neoversity/athlete_bio.csv
https://ftp.goit.study/neoversity/athlete_event_results.csv

## Архітектура Data Lake

Пайплайн побудовано за принципом:

landing --> bronze --> silver --> gold

## Landing

Сирі CSV-файли без змін:

landing/athlete_bio.csv
landing/athlete_event_results.csv

## Bronze

Дані зчитуються Spark та зберігаються у форматі Parquet:

bronze/athlete_bio
bronze/athlete_event_results

## Silver

Виконується очищення та дедублікація:
* очищення текстових колонок
* видалення дублікатів

silver/athlete_bio
silver/athlete_event_results

## Gold

Фінальна аналітична таблиця:

gold/avg_stats

Містить середні значення:

avg_weight
avg_height

Згруповані за:

sport
medal
sex
country_noc