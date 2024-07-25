package zencache

import (
	"encoding/json"
	"time"
)

type Item struct {
	Data       any       `json:"value"`
	Expiration time.Time `json:"expiration"`
}

func (i *Item) SetExpiration(expiration time.Duration) {
	if expiration != 0 {
		i.Expiration = time.Now().Add(expiration)
	}
}

func (i *Item) IsExpired() bool {
	return !i.Expiration.IsZero() && time.Now().After(i.Expiration)
}

func (i *Item) ParseData(data any) error {
	jsonBytes, err := json.Marshal(i.Data)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBytes, &data)
	if err != nil {
		return err
	}

	return nil
}

func NewItem(data any) *Item {
	return &Item{
		Data: data,
	}
}
