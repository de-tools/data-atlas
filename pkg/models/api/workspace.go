package api

type Workspace struct {
	Name string `json:"name"`
}

type WorkspaceResources struct {
	Resources []Resource `json:"resources"`
}
