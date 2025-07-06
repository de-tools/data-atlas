// API response types based on the backend models

export interface Workspace {
  name: string;
}

export interface WorkspaceResource {
  name: string;
}

export interface CostComponent {
  type: string;
  value: number;
  unit: string;
  totalAmount: number;
  rate: number;
  currency: string;
  description: string;
}

export interface ResourceDef {
  platform: string;
  service: string;
  name: string;
  description: string;
  tags: Record<string, string>;
  metadata: Record<string, string>;
}

export interface ResourceCost {
  startTime: string; // ISO date string
  endTime: string; // ISO date string
  resource: ResourceDef;
  costs: CostComponent[];
}
