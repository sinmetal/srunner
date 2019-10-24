package tweet

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	Update(ctx context.Context, id string) error
	Get(ctx context.Context, key spanner.Key) (*Tweet, error)
	GetTweet3Tables(ctx context.Context, key spanner.Key) ([]*Tweet, error)
	Query(ctx context.Context, limit int) ([]*Tweet, error)
	QueryHeavy(ctx context.Context) ([]*Tweet, error)
	QueryAll(ctx context.Context) (int, error)
	QueryResultStruct(ctx context.Context, limit int) ([]*TweetIDAndAuthor, error)
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(sc *spanner.Client) TweetStore {
	if tweetStore == nil {
		tweetStore = &defaultTweetStore{
			sc: sc,
		}
	}
	return tweetStore
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
		return errors.WithStack(err)
	}
	ms := []*spanner.Mutation{
		m,
	}

	_, err = s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
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
		return nil, errors.WithStack(err)
	}
	var tweet Tweet
	if err := row.ToStruct(&tweet); err != nil {
		return nil, errors.WithStack(err)
	}
	return &tweet, nil
}

func (s *defaultTweetStore) GetTweet3Tables(ctx context.Context, key spanner.Key) ([]*Tweet, error) {
	var results []*Tweet

	err := func() error {
		ctx, span := trace.StartSpan(ctx, "getTweet")
		defer span.End()

		row, err := s.sc.Single().ReadRow(ctx, s.TableName(), key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
		if err != nil {
			ecode := spanner.ErrCode(err)
			if ecode == codes.NotFound {
				return nil
			}
			return errors.WithStack(err)
		}
		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return errors.WithStack(err)
		}
		results = append(results, &tweet)
		return nil
	}()
	if err != nil {
		return nil, err
	}

	err = func() error {
		row, err := s.sc.Single().ReadRow(ctx, "TweetDummy1", key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
		if err != nil {
			ecode := spanner.ErrCode(err)
			if ecode == codes.NotFound {
				return nil
			}
			return errors.WithStack(err)
		}
		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return errors.WithStack(err)
		}
		results = append(results, &tweet)
		return nil
	}()
	if err != nil {
		return nil, err
	}

	err = func() error {
		row, err := s.sc.Single().ReadRow(ctx, "TweetDummy2", key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
		if err != nil {
			ecode := spanner.ErrCode(err)
			if ecode == codes.NotFound {
				return nil
			}
			return errors.WithStack(err)
		}
		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return errors.WithStack(err)
		}
		results = append(results, &tweet)
		return nil
	}()
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetStore) Query(ctx context.Context, limit int) ([]*Tweet, error) {
	ctx, span := startSpan(ctx, "query")
	defer span.End()

	iter := s.sc.Single().ReadUsingIndex(ctx, s.TableName(), "TweetSortAsc", spanner.AllKeys(), []string{"Id", "Sort"})
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
			return nil, errors.WithStack(err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return nil, errors.WithStack(err)
		}
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
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
			return nil, errors.WithStack(err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return nil, errors.WithStack(err)
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
			return 0, errors.WithStack(err)
		}

		var tweet Tweet
		if err := row.ToStruct(&tweet); err != nil {
			return 0, errors.WithStack(err)
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
func (s *defaultTweetStore) QueryResultStruct(ctx context.Context, limit int) ([]*TweetIDAndAuthor, error) {
	ctx, span := startSpan(ctx, "queryResultStruct")
	defer span.End()

	st := spanner.NewStatement("SELECT ARRAY(SELECT STRUCT(Id, Author)) As IdWithAuthor FROM Tweet LIMIT @LIMIT;")
	st.Params["LIMIT"] = limit
	iter := s.sc.Single().Query(ctx, st)
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
			return nil, errors.WithStack(err)
		}

		var result Result
		if err := row.ToStruct(&result); err != nil {
			return nil, errors.WithStack(err)
		}
		ias = append(ias, result.IDWithAuthor[0])
	}

	return ias, nil
}

func (s *defaultTweetStore) Update(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "update")
	defer span.End()

	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		tr, err := txn.ReadRow(ctx, s.TableName(), spanner.Key{id}, []string{"Count"})
		if err != nil {
			return err
		}

		var count int64
		if err := tr.ColumnByName("Count", &count); err != nil {
			return err
		}
		count++
		cols := []string{"Id", "Count", "UpdatedAt", "CommitedAt"}

		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Update(s.TableName(), cols, []interface{}{id, count, time.Now(), spanner.CommitTimestamp}),
		})
	})

	return err
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
