```
bq mk --project_id gcpug-public-spanner --connection --connection_type='CLOUD_SPANNER' --location='asia-northeast1' \
--properties='{"database":"projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal2", "useParallelism":true, "useDataBoost": true}' spanner_sinmetal2
```

# JOIN

Usersの子どもとしてOrdersがインターリーブされているので、JOINすることが可能

```
SELECT * FROM EXTERNAL_QUERY(
  'gcpug-public-spanner.asia-northeast1.spanner_sinmetal2',
  'SELECT Users.UserID,Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID') AS UserOrders
```

# Memo

sinmetalの雑多なメモ

UsersにインターリーブしているINDEXであれば、DataBoostで参照していても実行可能。
ただ、以下のようなクエリだとResidual Conditionになるので、処理の負荷を下げるのにさほど寄与しなくて、Table Full Scanすれば良いんじゃないか？って感じかも？

```
SELECT * FROM EXTERNAL_QUERY(
  'gcpug-public-spanner.asia-northeast1.spanner_sinmetal2',
  '''SELECT
       Users.UserID,
       Orders.OrderID,
       Orders.Amount,
       Orders.CommitedAt
     FROM Users JOIN Orders@{FORCE_INDEX=UserIDAndCommitedAtDescByOrdersParentUsers} ON Users.UserID = Orders.UserID
     WHERE FORMAT_TIMESTAMP("%Y%m",Orders.CommitedAt, "Asia/Tokyo") = "202309"''') AS UserOrders
```

```
+-----+---------------------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
| ID  | Query_Execution_Plan                                                                                                            | Rows_Returned | Executions | Total_Latency |
+-----+---------------------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
|   0 | Distributed Union (distribution_table: Users, split_ranges_aligned: true)                                                       | 19573         | 1          | 1.01 secs     |
|   1 | +- Local Distributed Union                                                                                                      | 19573         | 1          | 1.01 secs     |
|   2 |    +- Serialize Result                                                                                                          | 19573         | 1          | 1.01 secs     |
|   3 |       +- Cross Apply                                                                                                            | 19573         | 1          | 994.74 msecs  |
|  *4 |          +- [Input] Filter Scan                                                                                                 |               |            |               |
|   5 |          |  +- Index Scan (Full scan: true, Index: UserIDAndCommitedAtDescByOrdersStoredAmountParentUsers, scan_method: Scalar) | 19573         | 1          | 34.63 msecs   |
|  17 |          +- [Map] Local Distributed Union                                                                                       | 19573         | 19573      | 952.32 msecs  |
| *18 |             +- Filter Scan (seekable_key_size: 1)                                                                               | 19573         | 19573      | 941.31 msecs  |
|  19 |                +- Table Scan (Table: Users, scan_method: Scalar)                                                                | 19573         | 19573      | 924.97 msecs  |
+-----+---------------------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
Predicates(identified by ID):
  4: Residual Condition: (FORMAT_TIMESTAMP('%Y%m', $CommitedAt, 'Asia/Tokyo') = '202309')
 18: Seek Condition: ($UserID = $UserID_1)

19573 rows in set (1.71 secs)
timestamp:            2023-09-11T17:27:30.192549+09:00
cpu time:             349.2 msecs
rows scanned:         39146 rows
deleted rows scanned: 0 rows
optimizer version:    5
optimizer statistics: auto_20230906_07_18_51UTC
```