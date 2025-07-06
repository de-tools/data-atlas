import { exportToCSV } from "../../services/exportUtils";
import type { WorkspaceResource } from "../../types/api";
import type { TimeSeriesEntry } from "../../types/domain";

interface TableCostViewProps {
  timeSeriesData: TimeSeriesEntry[];
  sortedResources: WorkspaceResource[];
  loadingResources: string[];
  formatDate: (dateStr: string) => string;
  resourceTotals?: Record<string, number>;
  colors?: string[];
}

export function TableCostView({
  timeSeriesData,
  sortedResources,
  loadingResources,
  formatDate,
}: TableCostViewProps) {
  return (
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
          onClick={() => exportToCSV(timeSeriesData, sortedResources, formatDate)}
          className="rounded bg-green-500 px-4 py-2 text-white hover:bg-green-600"
        >
          Export to CSV
        </button>
      </div>
    </div>
  );
}
