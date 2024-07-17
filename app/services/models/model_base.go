package models

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

type ModelBase struct {
	gorm.Model
	ID        string `json:"id" gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

func (mb *ModelBase) BeforeCreate(tx *gorm.DB) error {
	mb.ID = uuid.New().String()
	return nil
}
