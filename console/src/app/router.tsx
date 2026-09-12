import { createBrowserRouter } from 'react-router-dom';
import { AppLayout } from './layout';
import { DecisionExplorerPage } from '../pages/DecisionExplorerPage';
import { ExperimentComparePage } from '../pages/ExperimentComparePage';
import { JobDetailPage } from '../pages/JobDetailPage';
import { OverviewPage } from '../pages/OverviewPage';
import { ResourceCenterPage } from '../pages/ResourceCenterPage';
import { SandboxViewPage } from '../pages/SandboxViewPage';
import { TimelinePage } from '../pages/TimelinePage';
import { TopologyPage } from '../pages/TopologyPage';
import { TraceExplorerPage } from '../pages/TraceExplorerPage';

export const router = createBrowserRouter([
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'jobs', element: <JobDetailPage /> },
      { path: 'jobs/:jobId', element: <JobDetailPage /> },
      { path: 'jobs/:jobId/timeline', element: <TimelinePage /> },
      { path: 'jobs/:jobId/topology', element: <TopologyPage /> },
      { path: 'jobs/:jobId/sandboxes', element: <SandboxViewPage /> },
      { path: 'jobs/:jobId/decisions', element: <DecisionExplorerPage /> },
      { path: 'jobs/:jobId/traces', element: <TraceExplorerPage /> },
      { path: 'experiments', element: <ExperimentComparePage /> },
      { path: 'timeline', element: <TimelinePage /> },
      { path: 'topology', element: <TopologyPage /> },
      { path: 'resources', element: <ResourceCenterPage /> },
      { path: 'sandboxes', element: <SandboxViewPage /> },
      { path: 'decisions', element: <DecisionExplorerPage /> },
      { path: 'traces', element: <TraceExplorerPage /> },
    ],
  },
]);
