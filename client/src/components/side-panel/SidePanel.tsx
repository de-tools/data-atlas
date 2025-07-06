import type { Workspace, WorkspaceResource } from "../../types/api";
import { ResourceSelector } from "./ResourceSelector";
import { TimeRangeSelector } from "./TimeRangeSelector";
import { WorkspaceSelector } from "./WorkspaceSelector";

interface SidePanelProps {
  selectedWorkspace: Workspace | null;
  selectedResources: WorkspaceResource[];
  interval: number;
  onWorkspaceSelect: (workspace: Workspace) => void;
  onResourcesSelect: (resources: WorkspaceResource[]) => void;
  onIntervalChange: (newInterval: number) => void;
}

export function SidePanel({
  selectedWorkspace,
  selectedResources,
  interval,
  onWorkspaceSelect,
  onResourcesSelect,
  onIntervalChange,
}: SidePanelProps) {
  return (
    <div className="col-span-1 space-y-6 md:col-span-1">
      <div className="rounded-lg bg-white p-6 shadow">
        <WorkspaceSelector
          onSelect={onWorkspaceSelect}
          selectedWorkspace={selectedWorkspace || undefined}
        />

        {selectedWorkspace && (
          <ResourceSelector
            workspace={selectedWorkspace}
            onSelectResources={onResourcesSelect}
            selectedResources={selectedResources}
          />
        )}

        <TimeRangeSelector onIntervalChange={onIntervalChange} defaultInterval={interval} />
      </div>
    </div>
  );
}
