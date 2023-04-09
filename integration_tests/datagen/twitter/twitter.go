package twitter

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"datagen/twitter/proto"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	protobuf "google.golang.org/protobuf/proto"
)

type tweetData struct {
	CreatedAt string `json:"created_at"`
	Id        string `json:"id"`
	Text      string `json:"text"`
	Lang      string `json:"lang"`
}

type twitterEvent struct {
	sink.BaseSinkRecord

	Data   tweetData   `json:"data"`
	Author twitterUser `json:"author"`
}

type twitterUser struct {
	CreatedAt string `json:"created_at"`
	Id        string `json:"id"`
	Name      string `json:"name"`
	UserName  string `json:"username"`
	Followers int    `json:"followers"`
}

func (r *twitterEvent) ToPostgresSql() string {
	return fmt.Sprintf("INSERT INTO tweet (created_at, id, text, lang, author_id) values ('%s', '%s', '%s', '%s', '%s'); INSERT INTO user (created_at, id, name, username, followers) values ('%s', '%s', '%s', '%s', %d);",
		r.Data.CreatedAt, r.Data.Id, r.Data.Text, r.Data.Lang, r.Author.Id,
		r.Author.CreatedAt, r.Author.Id, r.Author.Name, r.Author.UserName, r.Author.Followers,
	)
}

func (r *twitterEvent) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "twitter", r.Data.Id, data
}

func (r *twitterEvent) ToProtobuf() (topic string, key string, data []byte) {
	m := proto.Event{
		Data: &proto.TweetData{
			CreatedAt: r.Data.CreatedAt,
			Id:        r.Data.Id,
			Text:      r.Data.Text,
			Lang:      r.Data.Lang,
		},
		Author: &proto.User{
			CreatedAt: r.Author.CreatedAt,
			Id:        r.Author.Id,
			Name:      r.Author.Name,
			UserName:  r.Author.UserName,
			Followers: int64(r.Author.Followers),
		},
	}
	data, err := protobuf.Marshal(&m)
	if err != nil {
		panic(err)
	}
	return "twitter", r.Data.Id, data
}

func (r *twitterEvent) ToAvro() (topic string, key string, data []byte) {
	obj := map[string]interface{}{
		"data": map[string]interface{}{
			"created_at": r.Data.CreatedAt,
			"id":         r.Data.Id,
			"text":       r.Data.Text,
			"lang":       r.Data.Lang,
		},
		"author": map[string]interface{}{
			"created_at": r.Author.CreatedAt,
			"id":         r.Author.Id,
			"name":       r.Author.Name,
			"username":   r.Author.UserName,
			"followers":  r.Author.Followers,
		},
	}
	binary, err := AvroCodec.BinaryFromNative(nil, obj)
	if err != nil {
		panic(err)
	}
	return "twitter", r.Data.Id, binary
}

type twitterGen struct {
	faker *gofakeit.Faker
	users []*twitterUser
}

func NewTwitterGen() gen.LoadGenerator {
	faker := gofakeit.New(0)
	users := make(map[string]*twitterUser)
	for len(users) < 100000 {
		id := faker.DigitN(10)
		if _, ok := users[id]; !ok {
			endYear := time.Now().Year() - 1
			startYear := endYear - rand.Intn(8)

			endTime, _ := time.Parse("2006-01-01", fmt.Sprintf("%d-01-01", endYear))
			startTime, _ := time.Parse("2006-01-01", fmt.Sprintf("%d-01-01", startYear))
			users[id] = &twitterUser{
				CreatedAt: faker.DateRange(startTime, endTime).Format(gen.RwTimestampLayout),
				Id:        id,
				Name:      fmt.Sprintf("%s %s", faker.Name(), faker.Adverb()),
				UserName:  faker.Username(),
				Followers: gofakeit.IntRange(1, 100000),
			}
		}
	}
	usersList := []*twitterUser{}
	for _, u := range users {
		usersList = append(usersList, u)
	}
	return &twitterGen{
		faker: faker,
		users: usersList,
	}
}

func (t *twitterGen) generate() twitterEvent {
	id := t.faker.DigitN(19)
	author := t.users[rand.Intn(len(t.users))]

	wordsCnt := t.faker.IntRange(10, 20)
	hashTagsCnt := t.faker.IntRange(0, 2)
	hashTags := ""
	for i := 0; i < hashTagsCnt; i++ {
		hashTags += fmt.Sprintf("#%s ", t.faker.BuzzWord())
	}
	sentence := fmt.Sprintf("%s%s", hashTags, t.faker.Sentence(wordsCnt))
	return twitterEvent{
		Data: tweetData{
			Id:        id,
			CreatedAt: time.Now().Format(gen.RwTimestampLayout),
			Text:      sentence,
			Lang:      gofakeit.Language(),
		},
		Author: *author,
	}
}

func (t *twitterGen) KafkaTopics() []string {
	return []string{"twitter"}
}

func (t *twitterGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		record := t.generate()
		select {
		case <-ctx.Done():
			return
		case outCh <- &record:
		}
	}
}
