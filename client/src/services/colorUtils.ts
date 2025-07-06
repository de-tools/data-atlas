import type { WorkspaceResource } from "../types/api";

/**
 * Predefined colors for each resource type
 */
export const RESOURCE_COLORS: Record<string, string> = {
  Compute: "#4285F4", // Blue
  Storage: "#34A853", // Green
  "SQL Warehouse": "#9C27B0", // Purple
  Jobs: "#FF9800", // Orange
  Warehouse: "#EA4335", // Red
};

/**
 * Generates colors for resources based on predefined colors or a fallback algorithm
 * @param resources - Array of workspace resources
 * @returns Array of color strings corresponding to each resource
 */
export function generateResourceColors(resources: WorkspaceResource[]): string[] {
  return resources.map((resource) => {
    return RESOURCE_COLORS[resource.name] || `hsl(${(resource.name.length * 137) % 360}, 70%, 50%)`;
  });
}
