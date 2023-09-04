/*
+-----+---------------------------------------------------------------------------------------------------------------------+
| ID  | Query_Execution_Plan                                                                                                |
+-----+---------------------------------------------------------------------------------------------------------------------+
|   0 | Serialize Result                                                                                                    |
|   1 | +- Global Limit                                                                                                     |
|  *2 |    +- Distributed Cross Apply (order_preserving: true)                                                              |
|   3 |       +- [Input] Create Batch                                                                                       |
|   4 |       |  +- Compute Struct                                                                                          |
|  *5 |       |     +- Distributed Union (distribution_table: UserIDAndCommitedAtDescByOrders, split_ranges_aligned: false) |
|   6 |       |        +- Local Distributed Union                                                                           |
|  *7 |       |           +- Filter Scan                                                                                    |
|   8 |       |              +- Index Scan (Index: UserIDAndCommitedAtDescByOrders, scan_method: Scalar)                    |
|  24 |       +- [Map] Compute Struct                                                                                       |
|  25 |          +- MiniBatchKeyOrder                                                                                       |
|  26 |             +- Minor Sort Limit                                                                                     |
|  27 |                +- RowCount                                                                                          |
|  28 |                   +- Cross Apply                                                                                    |
|  29 |                      +- [Input] RowCount                                                                            |
|  30 |                      |  +- KeyRangeAccumulator                                                                      |
|  31 |                      |     +- Local Minor Sort                                                                      |
|  32 |                      |        +- MiniBatchAssign                                                                    |
|  33 |                      |           +- Batch Scan (Batch: $v3, scan_method: Scalar)                                    |
|  44 |                      +- [Map] Local Distributed Union                                                               |
| *45 |                         +- Filter Scan                                                                              |
|  46 |                            +- Table Scan (Table: OrderDetails, scan_method: Scalar)                                 |
+-----+---------------------------------------------------------------------------------------------------------------------+
Predicates(identified by ID):
  2: Split Range: ($OrderID_1 = $OrderID)
  5: Split Range: ($UserID = 'a')
  7: Seek Condition: ($UserID = 'a')
 45: Seek Condition: ($OrderID_1 = $sort_batched_OrderID)

 UserIDはOrders Table, ItemIDはOrderDetails TableにあるのでWHEREの条件はシンプルだが1つのINDEXにはできない
 UserIDでFilterしてOrdersを該当させた後、OrderDetailsをJOINさせると同時にItemIDをFilterする実行計画となった
 1UserのOrderの数が多くなると徐々に遅くなってしまうだろう
 */

EXPLAIN
SELECT
    Orders.OrderID,
    OrderDetailID,
    ItemID,
    Price,
    Quantity,
    Orders.CommitedAt
FROM
    OrderDetails JOIN Orders ON OrderDetails.OrderID = Orders.OrderID
WHERE
    Orders.UserID = "a"
ORDER BY Orders.CommitedAt DESC
LIMIT 30