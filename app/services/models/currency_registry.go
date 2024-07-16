package models

type RegistryMap map[string]Platforms

type Registry struct {
	Symbol           string    `json:"symbol" csv:"symbol"`
	PlatformsWithIds Platforms `json:"platforms_with_ids" csv:"platforms_with_ids"`
}
