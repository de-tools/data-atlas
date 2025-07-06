import { useEffect, useState } from "react";

import { fetchWorkspaces } from "../services/api";
import type { Workspace } from "../types/api";

interface WorkspaceSelectorProps {
  onSelect: (workspace: Workspace) => void;
  selectedWorkspace?: Workspace;
}

export function WorkspaceSelector({ onSelect, selectedWorkspace }: WorkspaceSelectorProps) {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [initialLoadComplete, setInitialLoadComplete] = useState(false);

  useEffect(() => {
    async function loadWorkspaces() {
      try {
        setLoading(true);
        const data = await fetchWorkspaces();
        setWorkspaces(data);

        if (data.length > 0 && !selectedWorkspace && !initialLoadComplete) {
          onSelect(data[0]);
          setInitialLoadComplete(true);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load workspaces");
      } finally {
        setLoading(false);
      }
    }

    loadWorkspaces();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (loading && workspaces.length === 0) {
    return <div className="p-4 text-center">Loading workspaces...</div>;
  }

  if (error) {
    return <div className="p-4 text-red-500">Error: {error}</div>;
  }

  if (workspaces.length === 0) {
    return <div className="p-4 text-center">No workspaces found</div>;
  }

  return (
    <div className="mb-6">
      <label htmlFor="workspace-select" className="mb-2 block text-sm font-medium">
        Select Workspace
      </label>
      <select
        id="workspace-select"
        className="w-full rounded-md border bg-white p-2"
        value={selectedWorkspace?.name || ""}
        onChange={(e) => {
          const selected = workspaces.find((ws) => ws.name === e.target.value);
          if (selected) {
            onSelect(selected);
          }
        }}
      >
        {workspaces.map((workspace) => (
          <option key={workspace.name} value={workspace.name}>
            {workspace.name}
          </option>
        ))}
      </select>
    </div>
  );
}
