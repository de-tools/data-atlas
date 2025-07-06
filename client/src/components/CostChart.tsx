import { useEffect, useMemo, useRef, useState } from "react";

import type { ResourceCost, WorkspaceResource } from "../types/api";

interface CostChartProps {
  costData: ResourceCost[];
  selectedResources: WorkspaceResource[];
  loadingResources?: string[];
}

type ViewType = "table" | "chart";

type TimeSeriesEntry = {
  date: string;
  [resourceName: string]: string | number;
};

function ViewSelector({
  activeView,
  onViewChange,
}: {
  activeView: ViewType;
  onViewChange: (view: ViewType) => void;
}) {
  return (
    <div className="mb-4 flex space-x-2">
      {(["chart", "table"] as ViewType[]).map((view) => (
        <button
          key={view}
          className={`rounded px-4 py-2 ${
            activeView === view
              ? "bg-blue-500 text-white"
              : "bg-gray-200 text-gray-700 hover:bg-gray-300"
          }`}
          onClick={() => onViewChange(view)}
        >
          {view === "table" ? "Table View" : "Chart View"}
        </button>
      ))}
    </div>
  );
}

export function CostChart({ costData, selectedResources, loadingResources = [] }: CostChartProps) {
  const [activeView, setActiveView] = useState<ViewType>("chart");
  const [hoveredPoint, setHoveredPoint] = useState<{
    resourceName: string;
    date: string;
    value: number;
    x: number;
    y: number;
  } | null>(null);

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    const day = date.getDate().toString().padStart(2, "0");
    const month = (date.getMonth() + 1).toString().padStart(2, "0");
    const year = date.getFullYear().toString().slice(2);
    return `${day}/${month}/${year}`;
  };

  const hoverTimeoutRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (hoverTimeoutRef.current !== null) {
        window.clearTimeout(hoverTimeoutRef.current);
      }
    };
  }, []);

  const handleMouseEnter = (point: {
    resourceName: string;
    date: string;
    value: number;
    x: number;
    y: number;
  }) => {
    if (hoverTimeoutRef.current !== null) {
      window.clearTimeout(hoverTimeoutRef.current);
    }

    hoverTimeoutRef.current = window.setTimeout(() => {
      setHoveredPoint(point);
    }, 50);
  };

  const handleMouseLeave = () => {
    if (hoverTimeoutRef.current !== null) {
      window.clearTimeout(hoverTimeoutRef.current);
    }

    hoverTimeoutRef.current = window.setTimeout(() => {
      setHoveredPoint(null);
    }, 100);
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

  const maxCostValue = useMemo(() => {
    if (!hasData || timeSeriesData.length === 0 || sortedResources.length === 0) return 0;

    let mx = 0;
    timeSeriesData.forEach((entry) =>
      sortedResources.forEach((res) => {
        mx = Math.max(mx, (entry[res.name] as number) || 0);
      }),
    );
    return mx;
  }, [hasData, timeSeriesData, sortedResources]);

  if (!hasData) {
    if (loadingResources.includes("initial-load")) {
      return (
        <div className="rounded-md border p-8 text-center">
          <p className="text-gray-500">Initializing cost explorer...</p>
        </div>
      );
    }

    if (loadingResources.length > 0) {
      return (
        <div className="rounded-md border p-8 text-center">
          <p className="text-gray-500">Loading cost data...</p>
        </div>
      );
    }

    if (selectedResources.length > 0) {
      return (
        <div className="rounded-md border p-8 text-center">
          <p className="text-gray-500">Fetching cost data for selected resources...</p>
        </div>
      );
    }

    return (
      <div className="rounded-md border p-8 text-center">
        <p>No cost data available. Select resources and a time range to view costs.</p>
      </div>
    );
  }

  const chartHeight = 300;
  const chartWidth = Math.max(timeSeriesData.length * 60, 600);

  // margins for axes & labels
  const margin = { top: 10, right: 20, bottom: 50, left: 50 };
  const innerWidth = chartWidth;
  const innerHeight = chartHeight;

  // scales
  const yDomainMax = maxCostValue * 1.1;
  const yScale = (v: number) => margin.top + innerHeight - (v / yDomainMax) * innerHeight;

  // Predefined colors for each resource type
  const RESOURCE_COLORS: Record<string, string> = {
    Compute: "#4285F4", // Blue
    Storage: "#34A853", // Green
    "SQL Warehouse": "#9C27B0", // Purple
    Jobs: "#FF9800", // Orange
    Warehouse: "#EA4335", // Red
  };

  const colors = sortedResources.map((resource) => {
    return RESOURCE_COLORS[resource.name] || `hsl(${(resource.name.length * 137) % 360}, 70%, 50%)`;
  });

  const renderChartView = () => (
    <div>
      <div className="relative" style={{ height: `${innerHeight + margin.top + margin.bottom}px` }}>
        <div className="absolute top-0 bottom-0 left-0" style={{ width: `${margin.left}px` }}>
          <svg
            width={margin.left}
            height={innerHeight + margin.top + margin.bottom}
            style={{ overflow: "visible" }}
          >
            <line
              x1={margin.left}
              y1={margin.top}
              x2={margin.left}
              y2={margin.top + innerHeight}
              stroke="#ccc"
            />

            {[yDomainMax, yDomainMax / 2, 0].map((tick) => {
              const y = yScale(tick);
              return (
                <g key={tick}>
                  <text
                    x={margin.left - 8}
                    y={y}
                    dy="0.32em"
                    textAnchor="end"
                    className="text-xs text-gray-500"
                  >
                    ${tick.toFixed(2)}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>

        <div
          className="absolute top-0 right-0 bottom-0 left-0 overflow-x-auto"
          style={{ marginLeft: `${margin.left}px` }}
        >
          <div
            style={{
              width: `${innerWidth}px`,
              height: `${innerHeight + margin.top + margin.bottom}px`,
              position: "relative",
            }}
          >
            <svg
              width={innerWidth}
              height={innerHeight + margin.top + margin.bottom}
              style={{ overflow: "visible" }}
            >
              {[yDomainMax, yDomainMax / 2, 0].map((tick) => {
                const y = yScale(tick);
                return (
                  <g key={`grid-${tick}`}>
                    <line x1={0} y1={y} x2={innerWidth} y2={y} stroke="#eee" />
                  </g>
                );
              })}

              <line
                x1={0}
                y1={margin.top + innerHeight}
                x2={innerWidth}
                y2={margin.top + innerHeight}
                stroke="#ccc"
                strokeWidth="1"
              />

              {sortedResources.map((res, idx) => {
                const d = timeSeriesData
                  .map((entry, i) => {
                    const x = (i / (timeSeriesData.length - 1)) * innerWidth;
                    const y = yScale((entry[res.name] as number) || 0);
                    return `${i === 0 ? "M" : "L"} ${x} ${y}`;
                  })
                  .join(" ");

                return (
                  <g key={`resource-${res.name}-${idx}`}>
                    <path d={d} fill="none" stroke={colors[idx]} strokeWidth={2} />
                    {timeSeriesData.map((entry, i) => {
                      const x = (i / (timeSeriesData.length - 1)) * innerWidth;
                      const y = yScale((entry[res.name] as number) || 0);
                      return (
                        <g key={`point-group-${res.name}-${i}`}>
                          <circle
                            key={`hit-area-${res.name}-${i}`}
                            cx={x}
                            cy={y}
                            r={10}
                            fill="transparent"
                            className="cursor-pointer"
                            onMouseEnter={() =>
                              handleMouseEnter({
                                resourceName: res.name,
                                date: entry.date,
                                value: (entry[res.name] as number) || 0,
                                x: x + margin.left,
                                y,
                              })
                            }
                            onMouseLeave={handleMouseLeave}
                          />
                          <circle
                            key={`point-${res.name}-${i}`}
                            cx={x}
                            cy={y}
                            r={5}
                            fill={colors[idx]}
                            className="hover:r-6 cursor-pointer transition-all duration-200"
                            pointerEvents="none"
                          />
                        </g>
                      );
                    })}
                  </g>
                );
              })}

              {timeSeriesData.map((entry, i) => {
                const x = (i / (timeSeriesData.length - 1)) * innerWidth;
                const y = margin.top + innerHeight + 15;
                return (
                  <text
                    key={entry.date}
                    x={x}
                    y={y}
                    textAnchor="middle"
                    transform={`rotate(-45, ${x}, ${y})`}
                    className="text-xs text-gray-500"
                    style={{ maxWidth: "40px", overflow: "hidden", textOverflow: "ellipsis" }}
                  >
                    {formatDate(entry.date)}
                  </text>
                );
              })}
            </svg>
          </div>
        </div>

        {hoveredPoint && (
          <div
            className="pointer-events-none absolute"
            style={{
              left: `${hoveredPoint.x < margin.left + 80 ? hoveredPoint.x + 20 : hoveredPoint.x > margin.left + innerWidth - 80 ? hoveredPoint.x - 160 : hoveredPoint.x - 70}px`,
              top: `${hoveredPoint.y < margin.top + 80 ? hoveredPoint.y + 20 : hoveredPoint.y - 80}px`,
              zIndex: 10,
            }}
          >
            <div
              className="rounded border border-gray-200 bg-white p-3 text-sm shadow-lg"
              style={{ width: "160px" }}
            >
              <div className="font-bold text-gray-800">{hoveredPoint.resourceName}</div>
              <div className="text-gray-600">Date: {formatDate(hoveredPoint.date)}</div>
              <div className="font-semibold text-gray-800">
                Cost: ${hoveredPoint.value.toFixed(2)}
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="mt-4 flex flex-wrap gap-4">
        {sortedResources.map((res, i) => (
          <div key={res.name} className="flex items-center">
            <div className="mr-2 h-4 w-4 rounded" style={{ backgroundColor: colors[i] }} />
            <span>
              {res.name}
              {loadingResources.includes(res.name) && (
                <span className="ml-2 inline-block animate-pulse text-gray-500">(Loading...)</span>
              )}
            </span>
          </div>
        ))}
      </div>
    </div>
  );

  const exportToCSV = () => {
    const headers = ["Date", ...sortedResources.map((r) => r.name)];
    const csvHeader = headers.join(",");

    const csvRows = timeSeriesData.map((entry) => {
      const date = formatDate(entry.date);
      const values = sortedResources.map((r) => `$${((entry[r.name] as number) || 0).toFixed(2)}`);
      return [date, ...values].join(",");
    });

    const csvString = [csvHeader, ...csvRows].join("\n");

    const blob = new Blob([csvString], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute("download", `cost-data-${new Date().toISOString().slice(0, 10)}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const renderTableView = () => (
    <div className="mt-4">
      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse">
          <thead>
            <tr className="bg-gray-100">
              <th className="border p-2 text-left">Date</th>
              {sortedResources.map((r) => (
                <th key={r.name} className="border p-2 text-left">
                  {r.name}
                  {loadingResources.includes(r.name) && (
                    <span className="ml-2 inline-block animate-pulse text-gray-500">
                      (Loading...)
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {timeSeriesData.map((entry, i) => (
              <tr key={i} className={i % 2 === 0 ? "bg-gray-50" : ""}>
                <td className="border p-2">{formatDate(entry.date)}</td>
                {sortedResources.map((r) => (
                  <td key={r.name} className="border p-2">
                    ${((entry[r.name] as number) || 0).toFixed(2)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="mt-4 flex justify-end">
        <button
          onClick={exportToCSV}
          className="rounded bg-green-500 px-4 py-2 text-white hover:bg-green-600"
        >
          Export to CSV
        </button>
      </div>
    </div>
  );

  return (
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
        {activeView === "chart" ? renderChartView() : renderTableView()}
      </div>
    </div>
  );
}
