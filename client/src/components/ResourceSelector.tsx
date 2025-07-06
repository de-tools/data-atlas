import { useEffect, useState } from "react";

import { fetchWorkspaceResources } from "../services/api";
import type { Workspace, WorkspaceResource } from "../types/api";

interface ResourceSelectorProps {
  workspace: Workspace;
  onSelectResources: (resources: WorkspaceResource[]) => void;
  selectedResources: WorkspaceResource[];
}

export function ResourceSelector({
  workspace,
  onSelectResources,
  selectedResources,
}: ResourceSelectorProps) {
  const [resources, setResources] = useState<WorkspaceResource[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function loadResources() {
      try {
        setLoading(true);
        setError(null);
        const data = await fetchWorkspaceResources(workspace.name);
        setResources(data);

        onSelectResources(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load resources");
      } finally {
        setLoading(false);
      }
    }

    if (workspace) {
      loadResources();
    }
  }, [workspace, onSelectResources]);

  const handleResourceToggle = (resource: WorkspaceResource) => {
    const isSelected = selectedResources.some((r) => r.name === resource.name);

    if (isSelected) {
      onSelectResources(selectedResources.filter((r) => r.name !== resource.name));
    } else {
      onSelectResources([...selectedResources, resource]);
    }
  };

  if (loading) {
    return <div className="p-4 text-center">Loading resources...</div>;
  }

  if (error) {
    return <div className="p-4 text-red-500">Error: {error}</div>;
  }

  if (resources.length === 0) {
    return <div className="p-4 text-center">No resources found for this workspace</div>;
  }

  return (
    <div className="mb-6">
      <h3 className="mb-2 text-lg font-medium">Select Resources</h3>
      <div className="h-60 space-y-2 overflow-y-auto rounded-md border p-2">
        {resources.map((resource) => {
          const isSelected = selectedResources.some((r) => r.name === resource.name);
          return (
            <div key={resource.name} className="flex items-center">
              <input
                type="checkbox"
                id={`resource-${resource.name}`}
                checked={isSelected}
                onChange={() => handleResourceToggle(resource)}
                className="mr-2"
              />
              <label htmlFor={`resource-${resource.name}`} className="cursor-pointer">
                {resource.name}
              </label>
            </div>
          );
        })}
      </div>
    </div>
  );
}
