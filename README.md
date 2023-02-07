# Служба такси

<div align="center">

![Cartax](images/logo.png)

</div>

## Описание проекта с требованиями
Есть таблица, состоящая из поездок такси в Нью-Йорке.

![Таблица](images/table.png)

Необходимо, используя таблицу поездок для каждого дня рассчитать процент поездок по количеству человек в машине (без пассажиров, 1, 2, 3, 4 и более пассажиров). Также добавить столбцы к предыдущим результатам с самой дорогой и самой дешевой поездкой для каждой группы.

По итогу должна получиться таблица (parquet) с колонками date, percentage_zero, percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus. Технологический стек – sql, scala (что-то одно).

**Дополнительно**: также провести аналитику и построить график на тему “как пройденное расстояние и количество пассажиров влияет на чаевые” в любом удобном инструменте.

## План реализации

### Используемые технологии
Технологический стек – Apache Spark, Scala, Kubernetes.

Kubernetes - оркестратор контейнеров промышленного уровня.

Spark - самый главный инструмент для трансформации данных. Он легко может загрузить csv файл и выгрузить результат в parquet.

Scala хороша тем, что это типизированный язык. Много синтаксических ошибок будет выявлено ещё на этапе компиляции.

В качестве файловой системы используется обычная файловая система хостовой машины.

### Схема

CSV файл кладется куда-то в локальную папку. Эта папка монтируется в Kubernetes. Оттуда его берет Spark и после некоторых трансформаций кладет parquet файлы обратно в эту папку.

![График](images/diagram.drawio.png)

### Настройка и запуск

[Статья](https://jaceklaskowski.github.io/spark-kubernetes-book/demo/spark-and-local-filesystem-in-minikube/) о том как монтировать локальную папку в Kubernetes.

minikube version: v1.28.0

<details>
  <summary>Пример</summary>

```bash
minikube start

# Билдим образ:
docker build -f ./docker/Dockerfile -t izair/taxi_service:1.0.5 .
docker push izair/taxi_service:1.0.5

# Заранее пулим:
minikube ssh docker pull izair/taxi_service:1.0.5

# Монтируем папку на minikube:
minikube mount /source_path:/tmp/taxi_service

# Проверим, что она там есть:
minikube ssh
ls /tmp/taxi_service
exit

# Потом смонтированную папку смонтируем на POD:
export VOLUME_TYPE=hostPath
export VOLUME_NAME=demo-host-mount
export MOUNT_PATH=/tmp/taxi_service

# Открываем порт 8001:
kubectl proxy

spark-submit \
  --master=k8s://http://127.0.0.1:8001 \
  --deploy-mode cluster \
  --name taxi_service \
  --class org.example.App \
  --conf "spark.kubernetes.container.image=izair/taxi_service:1.0.5" \
  --conf spark.kubernetes.driver.volumes.$VOLUME_TYPE.$VOLUME_NAME.mount.path=$MOUNT_PATH \
  --conf spark.kubernetes.driver.volumes.$VOLUME_TYPE.$VOLUME_NAME.options.path=$MOUNT_PATH \
  --conf spark.kubernetes.executor.volumes.$VOLUME_TYPE.$VOLUME_NAME.mount.path=$MOUNT_PATH \
  --conf spark.kubernetes.executor.volumes.$VOLUME_TYPE.$VOLUME_NAME.options.path=$MOUNT_PATH \
  --conf spark.executor.instances=1 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.driver.cores=1 \
  --conf spark.executor.cores=1 \
  --conf spark.kubernetes.namespace=default \
  local:///opt/taxi_service-1.0-jar-with-dependencies.jar

# Смотрим логи:
minikube dashboard
```
</details>

## Результаты разработки
В результате был создан проект со следующей структурой:
```bash
.
├── analysis                   # jupyter notebook analysis
├── data                       # data files
├── docker                     # docker files
├── docs                       # documentation
├── images                     # screenshots
├── src                        # source files
└── README.md
```

В папке data лежит файл head.csv с первыми несколькими  строками из [yellow_tripdata_2020-01.csv](https://disk.yandex.ru/d/DKeoopbGH1Ttuw) и parquet файл с результатом его обработки.

<details>
  <summary>Пример результата обработки</summary>

![График1](images/result1.png)![График2](images/result2.png)

</details>


## Выводы
На основании графиков можно сделать следующие выводы:
1. Если убрать выбросы, то видно, что постепенно горка опускается. Значит чем больше дистанция поездки, тем меньше чаевые. Возможно, люди думают, что если много заплатить за поездку, то чаевых можно не оставлять.

    ![График1](images/trip_distance.png)

2. Опять же, если убрать выбросы, то видно, что чем больше пассажиров, тем меньше чаевых они оставляли. Больше всего оставляет один человек.

    ![График2](images/passenger_count.png)




