import type { ResourceCost, Workspace, WorkspaceResource } from "../types/api";

// Mock workspaces
export const mockWorkspaces: Workspace[] = [
  { name: "Databricks Production" },
  { name: "Databricks Development" },
  { name: "Databricks Testing" },
];

// Mock resources for each workspace
export const mockResources: Record<string, WorkspaceResource[]> = {
  "Databricks Production": [
    { name: "Compute" },
    { name: "Storage" },
    { name: "SQL Warehouse" },
    { name: "Jobs" },
  ],
  "Databricks Development": [{ name: "Compute" }, { name: "Storage" }, { name: "SQL Warehouse" }],
  "Databricks Testing": [{ name: "Compute" }, { name: "Storage" }],
};

function generateDatesForPastDays(days: number): string[] {
  const dates: string[] = [];
  const today = new Date();

  for (let i = days; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(today.getDate() - i);
    dates.push(date.toISOString().split("T")[0]);
  }

  return dates;
}

function randomCost(min: number, max: number): number {
  return Math.round((Math.random() * (max - min) + min) * 100) / 100;
}

export function generateMockCostData(
  workspaceName: string,
  resourceName: string,
  interval: number = 7,
): ResourceCost[] {
  const dates = generateDatesForPastDays(interval);
  const result: ResourceCost[] = [];

  const costRanges: Record<string, { min: number; max: number }> = {
    Compute: { min: 50, max: 200 },
    Storage: { min: 10, max: 50 },
    "SQL Warehouse": { min: 100, max: 300 },
    Jobs: { min: 20, max: 80 },
    Warehouse: { min: 150, max: 400 },
  };

  const range = costRanges[resourceName] || { min: 10, max: 100 };

  for (let i = 0; i < dates.length - 1; i++) {
    const startDate = dates[i];
    const endDate = dates[i + 1];
    const totalCost = randomCost(range.min, range.max);

    const platform = workspaceName.includes("Databricks") ? "Azure" : "AWS";

    result.push({
      startTime: `${startDate}T00:00:00Z`,
      endTime: `${endDate}T00:00:00Z`,
      resource: {
        platform: platform,
        service: workspaceName.split(" ")[0],
        name: resourceName,
        description: `${resourceName} for ${workspaceName}`,
        tags: {
          environment: workspaceName.split(" ")[1],
          department: "Data Engineering",
        },
        metadata: {
          region: "us-west-2",
          accountId: "123456789012",
        },
      },
      costs: [
        {
          type: "compute",
          value: totalCost * 0.7,
          unit: "hours",
          totalAmount: totalCost * 0.7,
          rate: 0.5,
          currency: "USD",
          description: `${resourceName} compute usage`,
        },
        {
          type: "storage",
          value: totalCost * 0.3,
          unit: "GB",
          totalAmount: totalCost * 0.3,
          rate: 0.1,
          currency: "USD",
          description: `${resourceName} storage usage`,
        },
      ],
    });
  }

  return result;
}
