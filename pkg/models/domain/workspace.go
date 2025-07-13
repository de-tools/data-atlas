package domain

type Workspace struct {
	Name string
}

type WorkspaceResource struct {
	WorkspaceName string
	ResourceName  string
}

type WorkspaceResources struct {
	WorkspaceName string
	Resources     []string
}
