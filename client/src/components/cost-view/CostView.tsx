import { useMemo, useState } from "react";

import { generateResourceColors } from "../../services/colorUtils";
import type { ResourceCost, WorkspaceResource } from "../../types/api";
import { ChartCostView } from "./ChartCostView";
import { TableCostView } from "./TableCostView";
import { ViewSelector, type ViewType } from "./ViewSelector";

interface CostViewProps {
  costData: ResourceCost[];
  selectedResources: WorkspaceResource[];
  loadingResources?: string[];
  error?: string | null;
}

export type TimeSeriesEntry = {
  date: string;
  [resourceName: string]: string | number;
};

export function CostView({ costData, selectedResources, loadingResources = [], error }: CostViewProps) {
  const [activeView, setActiveView] = useState<ViewType>("chart");

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    const day = date.getDate().toString().padStart(2, "0");
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const year = date.getFullYear().toString().slice(2);
    return `${day}/${month}/${year}`;
  };

  const hasData = costData.length > 0;

  // Sort resources alphabetically by name
  const sortedResources = useMemo(() => {
    if (!hasData || selectedResources.length === 0) return selectedResources;

    return [...selectedResources].sort((a, b) => a.name.localeCompare(b.name));
  }, [hasData, selectedResources]);

  const resourceTotals = useMemo(() => {
    const totals: Record<string, number> = {};
    costData.forEach((cost) => {
      const name = cost.resource.name;
      totals[name] = (totals[name] || 0) + cost.costs.reduce((sum, c) => sum + c.totalAmount, 0);
    });
    return totals;
  }, [costData]);

  const timeSeriesData = useMemo((): TimeSeriesEntry[] => {
    const byDate: Record<string, Record<string, number>> = {};
    costData.forEach((cost) => {
      const date = new Date(cost.startTime).toISOString().slice(0, 10);
      const name = cost.resource.name;
      byDate[date] = byDate[date] || {};
      byDate[date][name] =
        (byDate[date][name] || 0) + cost.costs.reduce((sum, c) => sum + c.totalAmount, 0);
    });
    return Object.entries(byDate)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, values]) => ({ date, ...values }));
  }, [costData]);

  if (!hasData) {
    if (loadingResources.includes("initial-load")) {
      return (
        <div className="col-span-1 md:col-span-2">
          <div className="rounded-lg bg-white p-6 shadow">
            <div className="rounded-md border p-8 text-center">
              <p className="text-gray-500">Initializing cost explorer...</p>
            </div>
          </div>
        </div>
      );
    }

    if (loadingResources.length > 0) {
      return (
        <div className="col-span-1 md:col-span-2">
          <div className="rounded-lg bg-white p-6 shadow">
            <div className="rounded-md border p-8 text-center">
              <p className="text-gray-500">Loading cost data...</p>
            </div>
          </div>
        </div>
      );
    }

    if (selectedResources.length > 0) {
      return (
        <div className="col-span-1 md:col-span-2">
          <div className="rounded-lg bg-white p-6 shadow">
            <div className="rounded-md border p-8 text-center">
              <p className="text-gray-500">Fetching cost data for selected resources...</p>
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="col-span-1 md:col-span-2">
        <div className="rounded-lg bg-white p-6 shadow">
          <div className="rounded-md border p-8 text-center">
            <p>No cost data available. Select resources and a time range to view costs.</p>
          </div>
        </div>
      </div>
    );
  }

  const colors = generateResourceColors(sortedResources);

  return (
    <div className="col-span-1 md:col-span-2">
      <div className="rounded-lg bg-white p-6 shadow">
        {error ? (
          <div className="flex h-64 items-center justify-center">
            <p className="text-red-500">{error}</p>
          </div>
        ) : (
          <div className="rounded-md border p-4">
            <h3 className="mb-4 text-lg font-medium">Cost Visualization</h3>
            <ViewSelector activeView={activeView} onViewChange={setActiveView} />

            <div className="mb-6">
              <h4 className="text-md mb-2 font-medium">Total Cost by Resource</h4>
              <div className="space-y-2">
                {/* Create a combined list of all resources (both with data and loading) */}
                {(() => {
                  // Get all resource names (both with data and loading)
                  const allResourceNames = new Set([
                    ...sortedResources.map((res) => res.name),
                    ...loadingResources.filter((name) => !name.includes("initial-load")),
                  ]);

                  // Convert to array and sort alphabetically
                  return Array.from(allResourceNames)
                    .sort((a, b) => a.localeCompare(b))
                    .map((name) => {
                      const hasData = Object.keys(resourceTotals).includes(name);
                      const isLoading = loadingResources.includes(name);

                      return (
                        <div key={name} className="flex justify-between">
                          <span>
                            {name}
                            {isLoading && (
                              <span className="ml-2 inline-block animate-pulse text-gray-500">
                                (Loading...)
                              </span>
                            )}
                          </span>
                          <span className="font-medium">
                            {hasData ? `$${resourceTotals[name].toFixed(2)}` : "$0.00"}
                          </span>
                        </div>
                      );
                    });
                })()}
              </div>
            </div>

            <div>
              <h4 className="text-md mb-2 font-medium">Cost Over Time</h4>
              {activeView === "chart" ? (
                <ChartCostView
                  timeSeriesData={timeSeriesData}
                  sortedResources={sortedResources}
                  loadingResources={loadingResources}
                  formatDate={formatDate}
                  colors={colors}
                />
              ) : (
                <TableCostView
                  timeSeriesData={timeSeriesData}
                  sortedResources={sortedResources}
                  loadingResources={loadingResources}
                  formatDate={formatDate}
                />
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
