import type { WorkspaceResource } from "../types/api";
import type { TimeSeriesEntry } from "../types/domain";

/**
 * Exports time series data to a CSV file and triggers a download
 * @param timeSeriesData - Array of time series entries
 * @param resources - Array of workspace resources
 * @param formatDate - Function to format date strings
 */
export function exportToCSV(
  timeSeriesData: TimeSeriesEntry[],
  resources: WorkspaceResource[],
  formatDate: (dateStr: string) => string,
) {
  const headers = ["Date", ...resources.map((r) => r.name)];
  const csvHeader = headers.join(",");

  const csvRows = timeSeriesData.map((entry) => {
    const date = formatDate(entry.date);
    const values = resources.map((r) => `$${((entry[r.name] as number) || 0).toFixed(2)}`);
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
}
