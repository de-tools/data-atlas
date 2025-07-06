import { useState } from "react";

interface TimeRangeSelectorProps {
  onIntervalChange: (interval: number) => void;
  defaultInterval?: number;
}

const PREDEFINED_INTERVALS = [
  { label: "Last 7 days", value: 7 },
  { label: "Last 14 days", value: 14 },
  { label: "Last 30 days", value: 30 },
  { label: "Last 90 days", value: 90 },
];

export function TimeRangeSelector({
  onIntervalChange,
  defaultInterval = 7,
}: TimeRangeSelectorProps) {
  const [interval, setInterval] = useState(defaultInterval);
  const [customInterval, setCustomInterval] = useState("");
  const [isCustom, setIsCustom] = useState(false);

  const handleIntervalChange = (newInterval: number) => {
    setInterval(newInterval);
    onIntervalChange(newInterval);
    setIsCustom(false);
  };

  const handleCustomIntervalChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setCustomInterval(e.target.value);
  };

  const applyCustomInterval = () => {
    const parsedInterval = parseInt(customInterval, 10);
    if (!isNaN(parsedInterval) && parsedInterval > 0) {
      setInterval(parsedInterval);
      onIntervalChange(parsedInterval);
    }
  };

  return (
    <div className="mb-6">
      <h3 className="mb-2 text-lg font-medium">Select Time Range</h3>

      <div className="space-y-2">
        {PREDEFINED_INTERVALS.map((option) => (
          <div key={option.value} className="flex items-center">
            <input
              type="radio"
              id={`interval-${option.value}`}
              name="interval"
              checked={interval === option.value && !isCustom}
              onChange={() => handleIntervalChange(option.value)}
              className="mr-2"
            />
            <label htmlFor={`interval-${option.value}`} className="cursor-pointer">
              {option.label}
            </label>
          </div>
        ))}

        <div className="flex items-center">
          <input
            type="radio"
            id="interval-custom"
            name="interval"
            checked={isCustom}
            onChange={() => setIsCustom(true)}
            className="mr-2"
          />
          <label htmlFor="interval-custom" className="cursor-pointer">
            Custom:
          </label>
          <input
            type="number"
            min="1"
            value={customInterval}
            onChange={handleCustomIntervalChange}
            onFocus={() => setIsCustom(true)}
            className="ml-2 w-20 rounded border p-1"
            placeholder="days"
          />
          <button
            onClick={applyCustomInterval}
            disabled={!customInterval}
            className="ml-2 rounded bg-blue-500 px-2 py-1 text-white disabled:opacity-50"
          >
            Apply
          </button>
        </div>
      </div>

      <div className="mt-2 text-sm text-gray-600">Current selection: {interval} days</div>
    </div>
  );
}
