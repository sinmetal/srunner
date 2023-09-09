# インターリーブ比較体験

# Sample Data
```
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, "Nick", "Porter");
INSERT INTO Albums (SingerId, AlbumId, Title) VALUES (1, 1, "Total Junk"), (1, 2, "Nice Field");
```

```
EXPLAIN ANALYZE
SELECT * FROM Singers s
INNER JOIN Albums a ON s.SingerId = a.SingerId
WHERE s.SingerId = 1;
```

```
spanner-cli -p gcpug-public-spanner -i merpay-sponsored-instance -d $DB2 -e "$(cat query.sql)" -t
+-----+-----------------------------------------------------------------------------+---------------+------------+---------------+
| ID  | Query_Execution_Plan                                                        | Rows_Returned | Executions | Total_Latency |
+-----+-----------------------------------------------------------------------------+---------------+------------+---------------+
|  *0 | Distributed Union (distribution_table: Singers, split_ranges_aligned: true) | 2             | 1          | 0.14 msecs    |
|   1 | +- Local Distributed Union                                                  | 2             | 1          | 0.12 msecs    |
|   2 |    +- Serialize Result                                                      | 2             | 1          | 0.11 msecs    |
|   3 |       +- Cross Apply                                                        | 2             | 1          | 0.1 msecs     |
|  *4 |          +- [Input] Filter Scan (seekable_key_size: 1)                      | 1             | 1          | 0.05 msecs    |
|   5 |          |  +- Table Scan (Table: Singers, scan_method: Scalar)             | 1             | 1          | 0.05 msecs    |
|  13 |          +- [Map] Local Distributed Union                                   | 2             | 1          | 0.05 msecs    |
| *14 |             +- Filter Scan                                                  |               |            |               |
|  15 |                +- Table Scan (Table: Albums, scan_method: Scalar)           | 2             | 1          | 0.05 msecs    |
+-----+-----------------------------------------------------------------------------+---------------+------------+---------------+
Predicates(identified by ID):
  0: Split Range: ($SingerId = 1)
  4: Seek Condition: ($SingerId = 1)
 14: Seek Condition: ($SingerId_1 = 1)

2 rows in set (6.29 msecs)
timestamp:            2023-09-09T17:42:56.704156+09:00
cpu time:             5.12 msecs
rows scanned:         3 rows
deleted rows scanned: 0 rows
optimizer version:    5
optimizer statistics: auto_20230906_07_18_51UTC
```

インターリーブしてないPattern1と比べてSingersとAlbumsをJOINする部分がLocalで完結するようになっています。

# Refs

* https://spanner-hacks.apstn.dev/
* [Cloud Spanner でインターリーブテーブルを高速に取得する](https://medium.com/google-cloud-jp/cloud-spanner-%E3%81%A7%E3%82%A4%E3%83%B3%E3%82%BF%E3%83%BC%E3%83%AA%E3%83%BC%E3%83%96%E3%83%86%E3%83%BC%E3%83%96%E3%83%AB%E3%82%92%E9%AB%98%E9%80%9F%E3%81%AB%E5%8F%96%E5%BE%97%E3%81%99%E3%82%8B-2a955b061d3)