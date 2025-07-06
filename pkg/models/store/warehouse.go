package store

type Warehouse struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Size  string `json:"size"`
	State string `json:"state"`
}

type WarehousesResponse struct {
	Warehouses []Warehouse `json:"warehouses"`
}
