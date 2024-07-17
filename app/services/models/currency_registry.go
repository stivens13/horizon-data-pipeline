package models

type RegistryMap map[string]Platforms

func (rm *RegistryMap) ToRegistryView() (rv RegistryView) {
	for key, val := range *rm {
		rv.Data = append(rv.Data, &Registry{key, val})
	}

	return rv
}

type RegistryView struct {
	Data []*Registry
}

func (rv *RegistryView) ToRegistryMap() (rm RegistryMap) {
	rm = RegistryMap{}
	for _, r := range rv.Data {
		rm[r.Symbol] = r.PlatformsWithIds
	}
	return rm
}

type Registry struct {
	Symbol           string    `json:"symbol" csv:"symbol"`
	PlatformsWithIds Platforms `json:"platforms_with_ids" csv:"platforms_with_ids"`
}
