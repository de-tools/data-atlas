import { useCallback, useEffect, useRef, useState } from "react";

import { CostView } from "./components/cost-view/CostView";
import { SidePanel } from "./components/side-panel/SidePanel";
import { fetchResourceCost } from "./services/api";
import type { ResourceCost, Workspace, WorkspaceResource } from "./types/api";

function App() {
  const [selectedWorkspace, setSelectedWorkspace] = useState<Workspace | null>(null);
  const [selectedResources, setSelectedResources] = useState<WorkspaceResource[]>([]);
  const [interval, setInterval] = useState<number>(7);
  const [costData, setCostData] = useState<ResourceCost[]>([]);
  const [loadingResources, setLoadingResources] = useState<string[]>(["initial-load"]);
  const [error, setError] = useState<string | null>(null);

  const prevResourcesRef = useRef<WorkspaceResource[]>([]);
  const prevIntervalRef = useRef<number>(interval);

  const handleWorkspaceSelect = useCallback((workspace: Workspace) => {
    setSelectedWorkspace(workspace);
    setSelectedResources([]);
    prevResourcesRef.current = [];
  }, []);

  const handleResourcesSelect = useCallback((resources: WorkspaceResource[]) => {
    setSelectedResources(resources);
    setLoadingResources((prev) => (prev.includes("initial-load") ? [] : prev));
  }, []);

  const handleIntervalChange = useCallback((newInterval: number) => {
    setInterval(newInterval);
  }, []);

  useEffect(() => {
    async function fetchCostData() {
      if (!selectedWorkspace || selectedResources.length === 0) {
        if (!loadingResources.includes("initial-load")) {
          setCostData([]);
          prevResourcesRef.current = [];
        }
        return;
      }

      const prevResourceNames = new Set(prevResourcesRef.current.map((r) => r.name));
      const newResources = selectedResources.filter((r) => !prevResourceNames.has(r.name));

      const currentResourceNames = new Set(selectedResources.map((r) => r.name));

      const intervalChanged = prevIntervalRef.current !== interval;

      if (newResources.length === 0 && !intervalChanged) {
        if (prevResourcesRef.current.length > selectedResources.length) {
          setCostData((prev) =>
            prev.filter((item) => currentResourceNames.has(item.resource.name)),
          );
        }
        prevResourcesRef.current = [...selectedResources];
        return;
      }

      setLoadingResources(newResources.map((r) => r.name));
      setError(null);

      try {
        if (intervalChanged) {
          const allCostData: ResourceCost[] = [];

          for (const resource of selectedResources) {
            const data = await fetchResourceCost(selectedWorkspace.name, resource.name, interval);
            allCostData.push(...data);
          }

          setCostData(allCostData);
        } else {
          const newCostData: ResourceCost[] = [];

          for (const resource of newResources) {
            const data = await fetchResourceCost(selectedWorkspace.name, resource.name, interval);
            newCostData.push(...data);
          }

          setCostData((prev) => {
            const filtered = prev.filter((item) => currentResourceNames.has(item.resource.name));
            return [...filtered, ...newCostData];
          });
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch cost data");
      } finally {
        setLoadingResources([]);
        prevResourcesRef.current = [...selectedResources];
        prevIntervalRef.current = interval;
      }
    }

    fetchCostData();
  }, [selectedWorkspace, selectedResources, interval]);

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="mx-auto max-w-6xl">
        <header className="mb-8">
          <h1 className="text-3xl font-bold text-gray-800">Databricks Cost Explorer</h1>
          <p className="text-gray-600">Visualize and analyze your Databricks consumption costs</p>
        </header>

        <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
          <SidePanel
            selectedWorkspace={selectedWorkspace}
            selectedResources={selectedResources}
            interval={interval}
            onWorkspaceSelect={handleWorkspaceSelect}
            onResourcesSelect={handleResourcesSelect}
            onIntervalChange={handleIntervalChange}
          />

          <CostView
            costData={costData}
            selectedResources={selectedResources}
            loadingResources={loadingResources}
            error={error}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
