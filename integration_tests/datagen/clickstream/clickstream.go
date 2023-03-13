package clickstream

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type userBehavior struct {
	sink.BaseSinkRecord

	UserId         string `json:"user_id"`
	TargetId       string `json:"target_id"`
	TargetType     string `json:"target_type"`
	EventTimestamp string `json:"event_timestamp"`
	BehaviorType   string `json:"behavior_type"`

	// The two fields are used to express the following behaviors:
	// - Comment on a thread
	// - Comment on a comment.
	// Otherwise the fields will be empty.
	ParentTargetType string `json:"parent_target_type"`
	ParentTargetId   string `json:"parent_target_id"`
}

func (r *userBehavior) ToPostgresSql() string {
	return fmt.Sprintf(`INSERT INTO %s
(user_id, target_id, target_type, event_timestamp, behavior_type, parent_target_type, parent_target_id)
values ('%s', '%s', '%s', '%s', '%s', '%s', '%s')`,
		"user_behaviors", r.UserId, r.TargetId, r.TargetType, r.EventTimestamp, r.BehaviorType, r.ParentTargetType, r.ParentTargetId)
}

func (r *userBehavior) ToJson() (topic string, key string, data []byte) {
	data, _ = json.Marshal(r)
	return "user_behaviors", r.UserId, data
}

type targetType string

type clickStreamGen struct {
	faker *gofakeit.Faker
}

func NewClickStreamGen() gen.LoadGenerator {
	return &clickStreamGen{
		faker: gofakeit.New(0),
	}
}

func (g *clickStreamGen) randTargetType() targetType {
	switch p := g.faker.IntRange(0, 9); {
	case p < 7:
		return "thread"
	case p >= 7 && p < 9:
		return "comment"
	case p >= 9:
		return "user"
	default:
		panic(fmt.Sprintf("unreachable: %d", p))
	}
}

func (g *clickStreamGen) randBehaviorType(t targetType) string {
	switch t {
	case "thread":
		switch p := g.faker.IntRange(0, 99); {
		case p < 40:
			return "show"
		case p >= 40 && p < 65:
			return "upvote"
		case p >= 65 && p < 70:
			return "downvote"
		case p >= 70 && p < 75:
			return "share"
		case p >= 75 && p < 80:
			return "award"
		case p >= 80 && p < 90:
			return "save"
		case p >= 90:
			return "publish" // Publish a thread.
		default:
			panic(fmt.Sprintf("unreachable: %d", p))
		}
	case "comment":
		behaviors := []string{
			"publish", // Publish a comment, the parent target type can be a comment or a thread.
			"upvote",
			"downvote",
			"share",
			"award",
			"save",
		}
		return behaviors[g.faker.IntRange(0, len(behaviors)-1)]
	case "user":
		behaviors := []string{
			"show", // View the user profile.
			"follow",
			"share",
			"unfollow",
		}
		return behaviors[g.faker.IntRange(0, len(behaviors)-1)]
	default:
		panic("unexpected target type")
	}
}

func (g *clickStreamGen) generate() sink.SinkRecord {
	// TODO: The overall throughput can be further controlled by a scale factor.
	userId := g.faker.IntRange(0, 10)
	targetId := g.faker.IntRange(0, 100)
	target := g.randTargetType()
	behavior := g.randBehaviorType(target)
	// NOTE: The generated event might not be realistic, for example, a user is allowed to follow itself,
	// and a user can upvote a not existed thread. Anyway, it's just a simple demo.

	var parentTargetId string
	var parentTargetType string
	if target == "comment" && behavior == "publish" {
		possibleTargets := []string{"thread", "comment"}
		parentTargetType = possibleTargets[g.faker.IntRange(0, len(possibleTargets)-1)]
		parentTargetId = parentTargetType + fmt.Sprint(g.faker.IntRange(0, 100))
	}

	return &userBehavior{
		UserId:           fmt.Sprint(userId),
		TargetId:         string(target) + fmt.Sprint(targetId),
		TargetType:       string(target),
		EventTimestamp:   time.Now().Format(gen.RwTimestampLayout),
		BehaviorType:     behavior,
		ParentTargetType: parentTargetType,
		ParentTargetId:   parentTargetId,
	}
}

func (g *clickStreamGen) KafkaTopics() []string {
	return []string{"user_behaviors"}
}

func (g *clickStreamGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		record := g.generate()
		select {
		case <-ctx.Done():
			return
		case outCh <- record:
		}
	}
}
