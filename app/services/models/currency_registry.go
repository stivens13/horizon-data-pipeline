package models

type RegistryMap map[string]Platforms

type Registry struct {
	//Data []RegistryData
	Symbol           string    `json:"symbol" csv:"symbol"`
	PlatformsWithIds Platforms `json:"platforms_with_ids" csv:"platforms_with_ids"`
}

type RegistryData struct {
	Symbol           string    `json:"symbol" csv:"symbol"`
	PlatformsWithIds Platforms `json:"platforms_with_ids" csv:"platforms_with_ids"`
}

func (r *Registry) ToRegistryMap() (rm *RegistryMap) {
	rm = &RegistryMap{}
	return &RegistryMap{}
}

func ToRegistryMap(registry []Registry) (rm RegistryMap) {
	rm = RegistryMap{}
	for _, r := range registry {
		rm[r.Symbol] = r.PlatformsWithIds
	}

	return rm
}
