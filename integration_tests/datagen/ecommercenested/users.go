package ecommercenested

import (
	"context"
	"datagen/gen"
	"datagen/sink"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type organization struct {
	Name            string  `json:"name"`
	Address         address `json:"address"`
	Industry        string  `json:"industry"`           // TODO: write generator for that
	IsOutOfBusiness string  `json:"is_out_of_business"` // String on purpose. Cast it to bool in SQL
}

type organizationUserRelation struct {
	Role string
}

type user struct {
	Id       int64  `json:"user_id"`
	UserName string `json:"user_name"`
}

type userEvent struct {
	sink.BaseSinkRecord
	Id             int64                    `json:"user_event_id"`
	User           user                     `json:"user"`
	EventTimestamp string                   `json:"event_timestamp"`
	Org            organization             `json:"organization"`
	OrgRelation    organizationUserRelation `json:"organization_user_relation"`
	Email          string                   `json:"email"`
}

func (r userEvent) Key() string {
	return fmt.Sprint(r.Id)
}

func (r userEvent) ToJson() []byte {
	data, _ := json.Marshal(r)
	return data
}

func (r userEvent) Topic() string {
	return "user_events"
}

func getRandRole() string {
	roles := []string{"developer", "sales representative", "customer support agent", "human resources specialist", "marketing coordinator", "financial analyst", "project manager", "data scientist", "operations coordinator", "quality assurance tester"}
	return roles[rand.Intn(len(roles))]
}

func getRandIndustry() string {
	i := []string{"technology", "healthcare", "finance", "education", "retail", "entertainment", "automotive", "energy", "hospitality", "telecommunications", "real estate", "agriculture", "construction", "fashion", "media", "pharmaceuticals", "aviation", "sports", "logistics", "consulting"}
	return i[rand.Intn(len(i))]
}

// likelihood in percentage
func getRandIsOutOfBusiness(likelihood uint) string {
	if rand.Intn(100) < int(likelihood) {
		return "True"
	}
	return "False"
}

// I want to merge users and orders. orderEvents should reflect user IDs

type userGen struct {
	bankruptLikelihood int // likelihood in percentage that an organization is out of business
	seqUserId          int64
	faker              *gofakeit.Faker
	maxUserId          *atomic.Pointer[int64]
	users              []user
	orgs               []organization
}

func NewUserGen(maxId *atomic.Pointer[int64]) *userGen {
	ug := &userGen{
		bankruptLikelihood: 10,
		seqUserId:          0,
		maxUserId:          maxId,
		faker:              gofakeit.New(0),
		users:              []user{},
		orgs:               []organization{},
	}
	for i := 0; i < 1000; i++ {
		ug.users = append(ug.users, ug.getUser())
		ug.orgs = append(ug.orgs, organization{
			Name:            ug.faker.Company(),
			Address:         getAddress(ug.faker),
			Industry:        getRandIndustry(),
			IsOutOfBusiness: getRandIsOutOfBusiness(uint(ug.bankruptLikelihood)),
		})
	}

	return ug
}

func (g *userGen) getUser() user {
	g.seqUserId++
	g.maxUserId.Store(&g.seqUserId)
	// TODO: seqUserId and maxUserId should be the same
	// Need custom atomic Inc function for that
	return user{
		Id:       g.seqUserId,
		UserName: g.faker.Username(),
	}
}

func (g *userGen) getUserEvent() userEvent {
	r := rand.Intn(len(g.users))
	user := g.users[r]
	org := g.orgs[r]
	return userEvent{
		Id:             g.seqUserId,
		User:           user,
		EventTimestamp: time.Now().Format(gen.RwTimestampNaiveLayout),
		Org:            org,
		OrgRelation:    organizationUserRelation{Role: getRandRole()},
		Email:          fmt.Sprintf("%s@%s.com", user.UserName, org.Name),
	}
}

func (g *userGen) generate() []sink.SinkRecord {
	var records []sink.SinkRecord
	for i := 0; i < 100; i++ {
		records = append(records, g.getUserEvent())
	}
	return records
}

// implement a load function
func (g *userGen) Load(ctx context.Context, outCh chan<- sink.SinkRecord) {
	for {
		records := g.generate()
		for _, record := range records {
			select {
			case <-ctx.Done():
				return
			case outCh <- record:
			}
		}
		// TODO: be smarter about slowing down user event production
		time.Sleep(1 * time.Second)
	}
}
