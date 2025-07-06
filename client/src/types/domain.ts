/**
 * Represents a time series data entry with a date and dynamic resource values
 */
export type TimeSeriesEntry = {
  date: string;
  [resourceName: string]: string | number;
};
