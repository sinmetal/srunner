package tweet

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/sinmetal/srunner/internal/trace"
	"github.com/sinmetal/srunner/spanners"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// Store is TweetTable Functions
type Store interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) (time.Time, error)
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
}

var tweetStore Store

// NewStore is New TweetStore
func NewStore(sc *spanner.Client) Store {
	if tweetStore != nil {
		return tweetStore
	}
	return &defaultTweetStore{
		sc: sc,
	}
}

// Tweet is TweetTable Row
type Tweet struct {
	TweetID       string
	Author        string
	Content       string
	ContentLength int64 // generated columns
	Favos         []string
	Sort          int64
	ShardID       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CommitedAt    time.Time // allow_commit_timestamp
	SchemaVersion int64
}

// ToInsertOrUpdateMap is Tweet structから、InsertOrUpdateができないColumnを取り除いたmapを作る
//
// InsertOrUpdateができないColumnとしては、generated columnsで生成されるcolumnがある
func (t *Tweet) ToInsertOrUpdateMap() map[string]interface{} {
	shardID := int64(crc32.ChecksumIEEE([]byte(t.TweetID)) % 10)
	m := map[string]interface{}{
		"TweetID":       t.TweetID,
		"Author":        t.Author,
		"Content":       t.Content,
		"Favos":         t.Favos,
		"Sort":          t.Sort,
		"ShardID":       shardID,
		"CreatedAt":     t.CreatedAt,
		"UpdatedAt":     t.UpdatedAt,
		"CommitedAt":    spanner.CommitTimestamp,
		"SchemaVersion": t.SchemaVersion,
	}
	return m
}

type defaultTweetStore struct {
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetStore) TableName() string {
	return "Tweets"
}

// Insert is Insert to Tweet
func (s *defaultTweetStore) Insert(ctx context.Context, tweet *Tweet) (commitTimestamp time.Time, err error) {
	ctx, span := trace.StartSpan(ctx, "tweetstore.Insert")
	defer span.End()

	in := tweet.ToInsertOrUpdateMap()
	m := spanner.InsertMap(s.TableName(), in)
	ms := []*spanner.Mutation{
		m,
	}

	commitTimestamp, err = s.sc.Apply(ctx, ms, spanners.AppTransactionTagApplyOption())
	if err != nil {
		return time.Time{}, fmt.Errorf("failed spanner.Apply: %w", err)
	}

	return commitTimestamp, nil
}

func (s *defaultTweetStore) Get(ctx context.Context, key spanner.Key) (*Tweet, error) {
	ctx, span := trace.StartSpan(ctx, "tweetstore.Get")
	defer span.End()

	row, err := s.sc.Single().ReadRowWithOptions(ctx, s.TableName(), key,
		[]string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"},
		&spanner.ReadOptions{
			RequestTag: spanners.AppTag(),
		})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.Query")
	defer span.End()

	iter := s.sc.Single().WithTimestampBound(spanner.MaxStaleness(2*time.Second)).
		ReadWithOptions(ctx, s.TableName(), spanner.AllKeys(),
			[]string{"TweetId", "Sort"},
			&spanner.ReadOptions{
				Index:      "TweetBySort",
				RequestTag: spanners.AppTag(),
			})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.QueryRandom")
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

	iter := s.sc.Single().QueryWithOptions(ctx, spanner.NewStatement(fmt.Sprintf(sql)),
		spanner.QueryOptions{
			RequestTag: spanners.AppTag(),
		})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.QueryHeavy")
	defer span.End()

	iter := s.sc.Single().QueryWithOptions(ctx, spanner.NewStatement("SELECT * FROM Tweet WHERE Content Like  '%Hoge%' LIMIT 100"),
		spanner.QueryOptions{
			RequestTag: spanners.AppTag(),
		})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.QueryAll")
	defer span.End()

	iter := s.sc.Single().WithTimestampBound(spanner.ReadTimestamp(time.Now())).
		QueryWithOptions(ctx, spanner.NewStatement("SELECT * FROM Tweet"),
			spanner.QueryOptions{
				RequestTag: spanners.AppTag(),
			})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.QueryResultStruct")
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

	iter := roTx.QueryWithOptions(ctx, st, spanner.QueryOptions{
		RequestTag: spanners.AppTag(),
	})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.QueryOrderByCreatedAtDesc")
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
	iter := s.sc.Single().QueryWithOptions(ctx, st,
		spanner.QueryOptions{
			RequestTag: spanners.AppTag(),
		})
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
	ctx, span := trace.StartSpan(ctx, "tweetstore.Update")
	defer span.End()

	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		tr, err := txn.ReadRowWithOptions(ctx, s.TableName(),
			spanner.Key{id},
			[]string{"Count"},
			&spanner.ReadOptions{
				RequestTag: spanners.AppTag(),
			})
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
	}, spanner.TransactionOptions{
		CommitOptions:  spanner.CommitOptions{ReturnCommitStats: true},
		TransactionTag: spanners.AppTag(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed TweetStore.Update : %w", err)
	}
	span.SetAttributes(attribute.Int64("mutation-count", resp.CommitStats.GetMutationCount()))

	return &resp, nil
}

func (s *defaultTweetStore) UpdateDML(ctx context.Context, id string) (time.Time, error) {
	ctx, span := trace.StartSpan(ctx, "tweetstore.UpdateDML")
	defer span.End()

	resp, err := s.sc.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
	}, spanner.TransactionOptions{
		TransactionTag: spanners.AppTag(),
	})
	if err != nil {
		return time.Time{}, err
	}
	return resp.CommitTs, nil
}

// Delete is 指定した ID の Row を削除する
func (s *defaultTweetStore) Delete(ctx context.Context, id string) error {
	ctx, span := trace.StartSpan(ctx, "tweetstore.Delete")
	defer span.End()

	_, err := s.sc.Apply(ctx, []*spanner.Mutation{spanner.Delete(s.TableName(), spanner.Key{id})}, spanners.AppTransactionTagApplyOption())
	if err != nil {
		return err
	}
	return nil
}
