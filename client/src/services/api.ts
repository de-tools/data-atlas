import { generateMockCostData, mockResources, mockWorkspaces } from "../mocks/api-data";
import type { ResourceCost, Workspace, WorkspaceResource } from "../types/api";

// This is a mock implementation that returns predefined data
// In a real implementation, this would make API calls to the backend

export async function fetchWorkspaces(): Promise<Workspace[]> {
  // Simulate network delay
  await new Promise((resolve) => setTimeout(resolve, 200));

  // Return mock workspaces
  return [...mockWorkspaces];
}

export async function fetchWorkspaceResources(workspaceName: string): Promise<WorkspaceResource[]> {
  // Simulate network delay
  await new Promise((resolve) => setTimeout(resolve, 200));

  // Return mock resources for the specified workspace
  const resources = mockResources[workspaceName];
  if (!resources) {
    throw new Error(`No resources found for workspace ${workspaceName}`);
  }

  // Sort resources alphabetically by name
  return [...resources].sort((a, b) => a.name.localeCompare(b.name));
}

export async function fetchResourceCost(
  workspaceName: string,
  resourceName: string,
  interval: number = 7,
): Promise<ResourceCost[]> {
  // Simulate network delay
  await new Promise((resolve) => setTimeout(resolve, 200));

  // Generate and return mock cost data
  return generateMockCostData(workspaceName, resourceName, interval);
}
