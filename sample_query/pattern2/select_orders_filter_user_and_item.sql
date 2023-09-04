/*
+-----+-----------------------------------------------------------------------------------------------------------------------------+
| ID  | Query_Execution_Plan                                                                                                        |
+-----+-----------------------------------------------------------------------------------------------------------------------------+
|   0 | Serialize Result                                                                                                            |
|   1 | +- Global Limit                                                                                                             |
|  *2 |    +- Distributed Cross Apply (order_preserving: true)                                                                      |
|   3 |       +- [Input] Create Batch                                                                                               |
|   4 |       |  +- Compute Struct                                                                                                  |
|  *5 |       |     +- Distributed Union (distribution_table: UserIDAndCommitedAtDescByOrdersParentUsers, split_ranges_aligned: false) |
|   6 |       |        +- Local Distributed Union                                                                                   |
|  *7 |       |           +- Filter Scan                                                                                            |
|   8 |       |              +- Index Scan (Index: UserIDAndCommitedAtDescByOrdersParentUsers, scan_method: Scalar)                    |
|  24 |       +- [Map] Compute Struct                                                                                               |
|  25 |          +- MiniBatchKeyOrder                                                                                               |
|  26 |             +- Minor Sort Limit                                                                                             |
|  27 |                +- RowCount                                                                                                  |
|  28 |                   +- Cross Apply                                                                                            |
|  29 |                      +- [Input] RowCount                                                                                    |
|  30 |                      |  +- Local Minor Sort                                                                                 |
|  31 |                      |     +- MiniBatchAssign                                                                               |
|  32 |                      |        +- Batch Scan (Batch: $v3, scan_method: Scalar)                                               |
|  43 |                      +- [Map] Local Distributed Union                                                                       |
| *44 |                         +- Filter Scan (seekable_key_size: 2)                                                               |
|  45 |                            +- Table Scan (Table: OrderDetails, scan_method: Scalar)                                         |
+-----+-----------------------------------------------------------------------------------------------------------------------------+
Predicates(identified by ID):
  2: Split Range: ($OrderID_1 = $OrderID)
  5: Split Range: ($UserID = 'a')
  7: Seek Condition: ($UserID = 'a')
 44: Seek Condition: ($OrderID_1 = $sort_batched_OrderID)
 */

EXPLAIN
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
　　 Orders.UserID = "a"
ORDER BY
    Orders.CommitedAt DESC
LIMIT 30