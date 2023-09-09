SELECT *
FROM Singers s
INNER JOIN Albums a ON s.SingerId = a.SingerId
INNER JOIN Concerts c ON s.SingerId = c.SingerId
WHERE s.SingerId = 1;