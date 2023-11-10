package ecommercenested

import "math/rand"

type organization struct {
	Name            string  `json:"name"`
	Address         address `json:"address"`
	Industry        string  `json:"industry"`           // TODO: write generator for that
	IsOutOfBusiness string  `json:"is_out_of_business"` // String on purpose. Cast it to bool in SQL
}

type organizationUserRelation struct {
	Role string
}

type userEvent struct {
	User           organization             `json:"buyer"`
	EventTimestamp string                   `json:"event_timestamp"`
	Org            organization             `json:"organization"`
	OrgRelation    organizationUserRelation `json:"organization_user_relation"`
}

func getUserEvent() userEvent {
	return userEvent{}
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
