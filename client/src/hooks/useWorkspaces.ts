import { useEffect, useState } from "react";

import { fetchWorkspaces } from "../services/api";
import type { Workspace } from "../types/api";

export function useWorkspaces(
  onSelect: (workspace: Workspace) => void,
  selectedWorkspace?: Workspace
) {
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

  return { workspaces, loading, error };
}
