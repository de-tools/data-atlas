export type ViewType = "table" | "chart";

interface ViewSelectorProps {
  activeView: ViewType;
  onViewChange: (view: ViewType) => void;
}

export function ViewSelector({ activeView, onViewChange }: ViewSelectorProps) {
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
