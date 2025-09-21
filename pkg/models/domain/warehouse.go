package domain

// WarehouseMetadata represents detailed warehouse configuration information
type WarehouseMetadata struct {
	ID               string
	Name             string
	Size             string
	State            string
	MinNumClusters   int
	MaxNumClusters   int
	AutoStopMins     int
	EnableServerless bool
}
