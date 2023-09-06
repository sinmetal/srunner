```
cat query.sql
```

```
EXPLAIN ANALYZE
SELECT
    Orders.OrderID,
    Orders.CommitedAt,
    ARRAY(
        SELECT
    STRUCT<OrderDetailID STRING, ItemID STRING, Price INT64, Quantity INT64>
    (OrderDetailID,
      ItemID,
      Price,
      Quantity)) AS OrderDetails
FROM
    Orders JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
WHERE
　　 Orders.UserID = "ruby"
ORDER BY
    Orders.CommitedAt DESC
LIMIT 30
```

```
spanner-cli -p gcpug-public-spanner -i merpay-sponsored-instance -d sinmetal -e "$(cat query.sql)" -t
```

```
+-----+---------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
| ID  | Query_Execution_Plan                                                                                                | Rows_Returned | Executions | Total_Latency |
+-----+---------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
|   0 | Serialize Result                                                                                                    | 30            | 1          | 3.16 msecs    |
|   1 | +- Global Limit                                                                                                     | 30            | 1          | 3.12 msecs    |
|  *2 |    +- Distributed Cross Apply (order_preserving: true)                                                              | 30            | 1          | 3.12 msecs    |
|   3 |       +- [Input] Create Batch                                                                                       |               |            |               |
|   4 |       |  +- Compute Struct                                                                                          | 17            | 1          | 1.55 msecs    |
|  *5 |       |     +- Distributed Union (distribution_table: UserIDAndCommitedAtDescByOrders, split_ranges_aligned: false) | 17            | 1          | 1.54 msecs    |
|   6 |       |        +- Local Distributed Union                                                                           | 17            | 1          | 1.53 msecs    |
|  *7 |       |           +- Filter Scan                                                                                    |               |            |               |
|   8 |       |              +- Index Scan (Index: UserIDAndCommitedAtDescByOrders, scan_method: Scalar)                    | 17            | 1          | 1.51 msecs    |
|  24 |       +- [Map] Compute Struct                                                                                       | 30            | 1          | 1.43 msecs    |
|  25 |          +- MiniBatchKeyOrder                                                                                       |               |            |               |
|  26 |             +- Minor Sort Limit                                                                                     | 30            | 1          | 1.42 msecs    |
|  27 |                +- RowCount                                                                                          |               |            |               |
|  28 |                   +- Cross Apply                                                                                    | 268           | 1          | 1.31 msecs    |
|  29 |                      +- [Input] RowCount                                                                            |               |            |               |
|  30 |                      |  +- KeyRangeAccumulator                                                                      |               |            |               |
|  31 |                      |     +- Local Minor Sort                                                                      |               |            |               |
|  32 |                      |        +- MiniBatchAssign                                                                    |               |            |               |
|  33 |                      |           +- Batch Scan (Batch: $v3, scan_method: Scalar)                                    | 17            | 1          | 0.02 msecs    |
|  44 |                      +- [Map] Local Distributed Union                                                               | 268           | 17         | 1.24 msecs    |
| *45 |                         +- Filter Scan                                                                              |               |            |               |
|  46 |                            +- Table Scan (Table: OrderDetails, scan_method: Scalar)                                 | 268           | 17         | 1.2 msecs     |
+-----+---------------------------------------------------------------------------------------------------------------------+---------------+------------+---------------+
Predicates(identified by ID):
  2: Split Range: ($OrderID_1 = $OrderID)
  5: Split Range: ($UserID = 'ruby')
  7: Seek Condition: ($UserID = 'ruby')
 45: Seek Condition: ($OrderID_1 = $sort_batched_OrderID)

30 rows in set (22.31 msecs)
timestamp:            2023-09-06T17:32:58.357591+09:00
cpu time:             19.12 msecs
rows scanned:         285 rows
deleted rows scanned: 0 rows
optimizer version:    5
optimizer statistics: auto_20230831_22_27_47UTC
```