import { useEffect, useState } from "react";

import { fetchWorkspaceResources } from "../services/api";
import type { Workspace, WorkspaceResource } from "../types/api";

export function useWorkspaceResources(
  workspace: Workspace,
  onSelectResources: (resources: WorkspaceResource[]) => void
) {
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

  return { resources, loading, error };
}
