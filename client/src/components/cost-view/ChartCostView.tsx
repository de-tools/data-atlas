import { useEffect, useMemo, useRef, useState } from "react";

import type { WorkspaceResource } from "../../types/api";
import type { TimeSeriesEntry } from "../../types/domain";

interface ChartCostViewProps {
  timeSeriesData: TimeSeriesEntry[];
  sortedResources: WorkspaceResource[];
  loadingResources: string[];
  formatDate: (dateStr: string) => string;
  colors: string[];
  resourceTotals?: Record<string, number>;
}

export function ChartCostView({
  timeSeriesData,
  sortedResources,
  loadingResources,
  formatDate,
  colors,
}: ChartCostViewProps) {
  const [hoveredPoint, setHoveredPoint] = useState<{
    resourceName: string;
    date: string;
    value: number;
    x: number;
    y: number;
  } | null>(null);

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

  const chartHeight = 300;
  const chartWidth = Math.max(timeSeriesData.length * 60, 600);

  // margins for axes & labels
  const margin = { top: 10, right: 20, bottom: 50, left: 50 };
  const innerWidth = chartWidth;
  const innerHeight = chartHeight;

  // Calculate max value for y-axis scale
  const maxCostValue = useMemo(() => {
    if (timeSeriesData.length === 0 || sortedResources.length === 0) return 0;

    let mx = 0;
    timeSeriesData.forEach((entry) =>
      sortedResources.forEach((res) => {
        mx = Math.max(mx, (entry[res.name] as number) || 0);
      }),
    );
    return mx;
  }, [timeSeriesData, sortedResources]);

  // scales
  const yDomainMax = maxCostValue * 1.1;
  const yScale = (v: number) => margin.top + innerHeight - (v / yDomainMax) * innerHeight;

  return (
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
}
