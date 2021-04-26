package tweet

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/sinmetal/srunner/operation"
	"go.opencensus.io/trace"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// TweetStore is TweetTable Functions
type TweetStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) error
	InsertBench(ctx context.Context, id string) error
	InsertWithOperation(ctx context.Context, id string) error
	Update(ctx context.Context, id string) (*spanner.CommitResponse, error)
	UpdateDML(ctx context.Context, id string) (time.Time, error)
	Delete(ctx context.Context, id string) error
	Get(ctx context.Context, key spanner.Key) (*Tweet, error)
	Query(ctx context.Context, limit int) ([]*Tweet, error)
	QueryHeavy(ctx context.Context) ([]*Tweet, error)
	QueryAll(ctx context.Context) (int, error)
	QueryResultStruct(ctx context.Context, orderByAsc bool, limit int, tb *spanner.TimestampBound) ([]*TweetIDAndAuthor, error)
	QueryOrderByCreatedAtDesc(ctx context.Context, startShard int, endShard int, pageOption *PageOptionForQueryOrderByCreatedAtDesc, limit int) ([]*Tweet, error)
	QueryRandom(ctx context.Context) error
	QueryLatestByAuthor(ctx context.Context, author string, tb *spanner.TimestampBound) ([]*Tweet, error)
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(sc *spanner.Client) TweetStore {
	if tweetStore != nil {
		return tweetStore
	}
	return &defaultTweetStore{
		sc: sc,
	}
}

// Tweet is TweetTable Row
type Tweet struct {
	ID             string `spanner:"Id"`
	Author         string
	Content        string
	Count          int64
	Favos          []string
	Sort           int64
	ShardCreatedAt int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	SchemaVersion  int64
}

type defaultTweetStore struct {
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetStore) TableName() string {
	return "Tweet"
}

// Insert is Insert to Tweet
func (s *defaultTweetStore) Insert(ctx context.Context, tweet *Tweet) error {
	ctx, span := startSpan(ctx, "insert")
	defer span.End()

	m, err := spanner.InsertStruct(s.TableName(), tweet)
	if err != nil {
		return fmt.Errorf("failed spanner.InsertStruct: %w", err)
	}
	ms := []*spanner.Mutation{
		m,
	}

	_, err = s.sc.Apply(ctx, ms)
	if err != nil {
		return fmt.Errorf("failed spanner.Apply: %w", err)
	}

	return nil
}

// InsertWithOperation is Tweet Table と Operation Table に Insertを行う
func (s *defaultTweetStore) InsertWithOperation(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "insertWithOperation")
	defer span.End()

	ml := []*spanner.Mutation{}
	now := time.Now()

	shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
	t := &Tweet{
		ID:             id,
		Content:        id,
		Favos:          []string{},
		ShardCreatedAt: int64(shardId),
		CreatedAt:      now,
		UpdatedAt:      now,
		CommitedAt:     spanner.CommitTimestamp,
	}
	tm, err := spanner.InsertStruct(s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tm)

	tom, err := operation.NewOperationInsertMutation(uuid.New().String(), "INSERT", "", s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tom)

	_, err = s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(ml)
	})

	return err
}

func (s *defaultTweetStore) Get(ctx context.Context, key spanner.Key) (*Tweet, error) {
	ctx, span := startSpan(ctx, "get")
	defer span.End()

	row, err := s.sc.Single().ReadRow(ctx, s.TableName(), key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
	if err != nil {
		ecode := spanner.ErrCode(err)
		if ecode == codes.NotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed spanner.ReadRow: %w", err)
	}
	var tweet Tweet
	if err := row.ToStruct(&tweet); err != nil {
		return nil, fmt.Errorf("failed spanner.Row.ToStruct: %w", err)
	}
	return &tweet, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetStore) Query(ctx context.Context, limit int) ([]*Tweet, error) {
	ctx, span := startSpan(ctx, "query")
	defer span.End()

	iter := s.sc.Single().WithTimestampBound(spanner.MaxStaleness(2*time.Second)).ReadUsingIndex(ctx, s.TableName(), "TweetSortAsc", spanner.AllKeys(), []string{"Id", "Sort"})
	defer iter.Stop()

	count := 0
	tweets := []*Tweet{}
	for {
		if count >= limit {
			return tweets, nil
		}
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return nil, fmt.Errorf("failed spanner.Row.ToStruct : %w", err)
		}
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}

// QueryRandom is Query Statsを水増しするための適当なクエリを実行する
func (s *defaultTweetStore) QueryRandom(ctx context.Context) error {
	ctx, span := startSpan(ctx, "queryRandom")
	defer span.End()

	sql := fmt.Sprintf(`
SELECT * 
FROM (
  SELECT * 
  FROM ItemOrder 
  WHERE UserID = "sinmetal" 
  ORDER BY CommitedAt DESC 
  Limit 10
) IO 
JOIN (
  SELECT * 
  FROM ItemMaster 
  WHERE Price > %v) IM ON IO.ItemID = IM.ItemID
JOIN User U ON IO.UserID = U.UserID
`, rand.Int63())

	iter := s.sc.Single().Query(ctx, spanner.NewStatement(fmt.Sprintf(sql)))
	defer iter.Stop()

	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}
	}

	return nil
}

func (s *defaultTweetStore) QueryHeavy(ctx context.Context) ([]*Tweet, error) {
	ctx, span := startSpan(ctx, "queryHeavy")
	defer span.End()

	iter := s.sc.Single().Query(ctx, spanner.NewStatement("SELECT * FROM Tweet WHERE Content Like  '%Hoge%' LIMIT 100"))
	defer iter.Stop()

	count := 0
	tweets := []*Tweet{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return nil, fmt.Errorf("failed spanner.Row.ToStruct : %w", err)
		}
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}

// QueryAll is 全件ひたすら取得して、件数を返す
func (s *defaultTweetStore) QueryAll(ctx context.Context) (int, error) {
	ctx, span := startSpan(ctx, "queryAll")
	defer span.End()

	iter := s.sc.Single().WithTimestampBound(spanner.ReadTimestamp(time.Now())).Query(ctx, spanner.NewStatement("SELECT * FROM Tweet"))
	defer iter.Stop()

	count := 0
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return 0, fmt.Errorf("failed spanner.Row.ToStruct : %w", err)
		}
		count++
	}

	return count, nil
}

// TweetIDAndAuthor is StructのResponseの確認用に作ったStruct
type TweetIDAndAuthor struct {
	ID     string `spanner:"Id"`
	Author string
}

// QueryResultStruct is StructをResultで返すQueryのサンプル
func (s *defaultTweetStore) QueryResultStruct(ctx context.Context, orderByAsc bool, limit int, timestampBound *spanner.TimestampBound) ([]*TweetIDAndAuthor, error) {
	ctx, span := startSpan(ctx, "queryResultStruct")
	defer span.End()

	sql := `
SELECT ARRAY(SELECT STRUCT(Id, Author)) As IdWithAuthor 
FROM Tweet@{FORCE_INDEX=TweetShardCreatedAtAscCreatedAtDesc} 
WHERE ShardCreatedAt = @ShardCreatedAt
`
	if orderByAsc {
		sql += " ORDER BY CreatedAt"
	} else {
		sql += " ORDER BY CreatedAt DESC"
	}
	sql += " LIMIT @Limit;"

	shardCreatedAt := rand.Intn(10)
	st := spanner.NewStatement(sql)
	st.Params["ShardCreatedAt"] = shardCreatedAt
	st.Params["Limit"] = limit
	roTx := s.sc.Single()
	if timestampBound != nil {
		roTx = roTx.WithTimestampBound(*timestampBound)
	}

	iter := roTx.Query(ctx, st)
	defer iter.Stop()

	type Result struct {
		IDWithAuthor []*TweetIDAndAuthor `spanner:"IdWithAuthor"`
	}

	ias := []*TweetIDAndAuthor{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}

		var result Result
		if err := row.ToStruct(&result); err != nil {
			return nil, fmt.Errorf("failed spanner.Row.ToStruct : %w", err)
		}
		ias = append(ias, result.IDWithAuthor[0])
	}

	return ias, nil
}

type PageOptionForQueryOrderByCreatedAtDesc struct {
	ID        string
	CreatedAt time.Time
}

// QueryResultStruct is StructをResultで返すQueryのサンプル
func (s *defaultTweetStore) QueryOrderByCreatedAtDesc(ctx context.Context, startShard int, endShard int, pageOption *PageOptionForQueryOrderByCreatedAtDesc, limit int) ([]*Tweet, error) {
	ctx, span := startSpan(ctx, "queryOrderByCreatedAtDesc")
	defer span.End()

	sql := `
SELECT c.Id, c.Author, c.Content, c.Count, c.Favos, c.Sort, c.ShardCreatedAt, c.CreatedAt, c.UpdatedAt, c.CommitedAt, c.SchemaVersion 
FROM UNNEST(GENERATE_ARRAY(@StartShard, @EndShard)) AS OneShardCreatedAt,
     UNNEST(ARRAY(
      SELECT AS STRUCT *
      FROM Tweet@{FORCE_INDEX=TweetShardCreatedAtAscCreatedAtDesc} 
      WHERE ShardCreatedAt = OneShardCreatedAt
      ORDER BY CreatedAt DESC LIMIT @Limit
    )) AS c
ORDER BY c.CreatedAt DESC, Id
LIMIT @Limit
`

	st := spanner.NewStatement(sql)
	st.Params["StartShard"] = startShard
	st.Params["EndShard"] = endShard
	st.Params["Id"] = pageOption.ID
	st.Params["Limit"] = limit
	iter := s.sc.Single().Query(ctx, st)
	defer iter.Stop()

	ts := []*Tweet{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed spanner.Iterator.Next : %w", err)
		}

		var t Tweet
		if err := row.ToStruct(&t); err != nil {
			return nil, fmt.Errorf("failed spanner.Row.ToStruct : %w", err)
		}
		ts = append(ts, &t)
	}

	return ts, nil
}

func (s *defaultTweetStore) Update(ctx context.Context, id string) (*spanner.CommitResponse, error) {
	ctx, span := startSpan(ctx, "update")
	defer span.End()

	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		tr, err := txn.ReadRow(ctx, s.TableName(), spanner.Key{id}, []string{"Count"})
		if err != nil {
			return fmt.Errorf("failed spanner.ReadRow : %w", err)
		}

		var count int64
		if err := tr.ColumnByName("Count", &count); err != nil {
			return fmt.Errorf("failed spanner.ColumnByName : %w", err)
		}
		count++
		cols := []string{"Id", "Count", "UpdatedAt", "CommitedAt"}

		err = txn.BufferWrite([]*spanner.Mutation{
			spanner.Update(s.TableName(), cols, []interface{}{id, count, time.Now(), spanner.CommitTimestamp}),
		})
		if err != nil {
			return fmt.Errorf("failed spanner.Tx.BufferWrite : %w", err)
		}
		return nil
	}, spanner.TransactionOptions{CommitOptions: spanner.CommitOptions{ReturnCommitStats: true}})
	if err != nil {
		return nil, fmt.Errorf("failed TweetStore.Update : %w", err)
	}
	span.AddAttributes(trace.Int64Attribute("mutation-count", resp.CommitStats.GetMutationCount()))

	return &resp, nil
}

func (s *defaultTweetStore) UpdateDML(ctx context.Context, id string) (time.Time, error) {
	ctx, span := startSpan(ctx, "updateDML")
	defer span.End()

	return s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `UPDATE Tweet SET Count += 1, UpdatedAt = @UpdatedAt, CommitedAt = PENDING_COMMIT_TIMESTAMP() WHERE Id = @Id`,
		}
		stmt.Params = map[string]interface{}{
			"@UpdatedAt": time.Now(),
			"@Id":        id,
		}
		_, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		return nil
	})
}

// Delete is 指定した ID の Row を削除する
func (s *defaultTweetStore) Delete(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "delete")
	defer span.End()

	_, err := s.sc.Apply(ctx, []*spanner.Mutation{spanner.Delete(s.TableName(), spanner.Key{id})})
	if err != nil {
		return err
	}
	return nil
}

// InsertBench is 複数TableへのInsertを行う
func (s *defaultTweetStore) InsertBench(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "insertbench")
	defer span.End()

	ml := []*spanner.Mutation{}
	now := time.Now()

	shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
	t := &Tweet{
		ID:             id,
		Content:        id,
		Favos:          []string{},
		ShardCreatedAt: int64(shardId),
		CreatedAt:      now,
		UpdatedAt:      now,
		CommitedAt:     spanner.CommitTimestamp,
	}
	tm, err := spanner.InsertStruct(s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tm)

	tom, err := operation.NewOperationInsertMutation(uuid.New().String(), "INSERT", "", s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tom)

	for i := 1; i < 4; i++ {
		td := &Tweet{
			ID:             id,
			Content:        id,
			Favos:          []string{},
			ShardCreatedAt: int64(shardId),
			CreatedAt:      now,
			UpdatedAt:      now,
			CommitedAt:     spanner.CommitTimestamp,
		}
		tdm, err := spanner.InsertStruct(fmt.Sprintf("TweetDummy%d", i), td)
		if err != nil {
			return err
		}
		ml = append(ml, tdm)
	}
	_, err = s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(ml)
	})

	return err
}

// InsertBench is 複数TableへのInsertを行う
func (s *defaultTweetStore) QueryLatestByAuthor(ctx context.Context, author string, tb *spanner.TimestampBound) ([]*Tweet, error) {
	stm := spanner.NewStatement(
		`SELECT Id, Author, Content, Count, Favos, Sort, ShardCreatedAt, CreatedAt, UpdatedAt, CommitedAt, SchemaVersion 
FROM Tweet WHERE Author = @Author ORDER BY CreatedAt DESC LIMIT @Limit`)
	stm.Params["Author"] = author
	stm.Params["Limit"] = 50

	var results []*Tweet
	roTx := s.sc.Single()
	if tb != nil {
		roTx = roTx.WithTimestampBound(*tb)
	}
	iter := roTx.Query(ctx, stm)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return results, nil
		} else if err != nil {
			return nil, err
		}

		t := &Tweet{}
		if err := row.ToStruct(t); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
}
