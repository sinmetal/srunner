# DataBoost

https://cloud.google.com/spanner/docs/databoost/databoost-overview

```
bq mk --project_id gcpug-public-spanner --connection --connection_type='CLOUD_SPANNER' --location='asia-northeast1' \
--properties='{"database":"projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal1", "useParallelism":true, "useDataBoost": true}' spanner_sinmetal1
```

# JOIN

DataBoostでSpannerに対して実行するQueryはPartitionQueryとして実行される。
そのため、実行できるQueryに制限が存在する。
pattern1の場合、UsersとOrdersはインターリーブされてないので、JOINすると実行不可能となる。

```
# このクエリは実行できない
SELECT * FROM EXTERNAL_QUERY(
  'gcpug-public-spanner.asia-northeast1.spanner_sinmetal1',
  'SELECT Users.UserID,Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID') AS Users
```