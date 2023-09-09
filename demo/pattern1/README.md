```
EXPLAIN ANALYZE
WITH
  TargetOrders AS (
  SELECT
    Orders.OrderID,
    Orders.CommitedAt,
  FROM
    Orders
  WHERE
    Orders.UserID = "ruby"
  ORDER BY
    Orders.CommitedAt DESC
  LIMIT
    30 )
SELECT
  Orders.OrderID,
  Orders.CommitedAt,
   ARRAY(
     SELECT STRUCT<OrderDetailID STRING, ItemID STRING, Price INT64, Quantity INT64>
     (OrderDetailID,
      ItemID,
      Price,
      Quantity)) AS OrderDetails
FROM TargetOrders AS Orders JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
```

```
spanner-cli -p gcpug-public-spanner -i merpay-sponsored-instance -d sinmetal -e "$(cat query.sql)" -t                                                                                                                                                                1 ↵
```

```
+-----+------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
| ID  | Query_Execution_Plan                                                                                             | Rows_Returned | Executions | Total_Latency |
+-----+------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
|  *0 | Distributed Cross Apply                                                                                          | 469           | 1          | 32.93 msecs   |
|   1 | +- [Input] Create Batch                                                                                          |               |            |               |
|   2 | |  +- Compute Struct                                                                                             | 30            | 1          | 3 msecs       |
|   3 | |     +- Global Limit                                                                                            | 30            | 1          | 2.97 msecs    |
|  *4 | |        +- Distributed Union (distribution_table: UserIDAndCommitedAtDescByOrders, split_ranges_aligned: false) | 30            | 1          | 2.97 msecs    |
|   5 | |           +- Local Limit                                                                                       | 30            | 1          | 2.95 msecs    |
|   6 | |              +- Local Distributed Union                                                                        | 30            | 1          | 2.95 msecs    |
|  *7 | |                 +- Filter Scan                                                                                 |               |            |               |
|   8 | |                    +- Index Scan (Index: UserIDAndCommitedAtDescByOrders, scan_method: Scalar)                 | 30            | 1          | 2.94 msecs    |
|  24 | +- [Map] Serialize Result                                                                                        | 469           | 1          | 29.71 msecs   |
|  25 |    +- Compute Struct                                                                                             | 469           | 1          | 29.24 msecs   |
|  26 |       +- Cross Apply                                                                                             | 469           | 1          | 29.05 msecs   |
|  27 |          +- [Input] KeyRangeAccumulator                                                                          |               |            |               |
|  28 |          |  +- Batch Scan (Batch: $v3, scan_method: Scalar)                                                      |               |            |               |
|  31 |          +- [Map] Local Distributed Union                                                                        | 469           | 30         | 28.98 msecs   |
| *32 |             +- Filter Scan                                                                                       |               |            |               |
|  33 |                +- Table Scan (Table: OrderDetails, scan_method: Scalar)                                          | 469           | 30         | 28.93 msecs   |
+-----+------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
Predicates(identified by ID):
  0: Split Range: ($OrderID_3 = $OrderID)
  4: Split Range: ($UserID = 'ruby')
  7: Seek Condition: ($UserID = 'ruby')
 32: Seek Condition: ($OrderID_3 = $batched_OrderID)

469 rows in set (60.15 msecs)
timestamp:            2023-09-07T20:12:35.261622+09:00
cpu time:             11.31 msecs
rows scanned:         499 rows
deleted rows scanned: 0 rows
optimizer version:    5
optimizer statistics: auto_20230906_04_27_19UTC
```