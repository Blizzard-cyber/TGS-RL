import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { AppLayoutWithClient } from '../app/layout';
import { resources } from '../api/mockFixture';
import { createTestApiClient, ready } from '../test/testApiClient';
import { ResourceCenterPage } from './ResourceCenterPage';

function renderPage() {
  const router = createMemoryRouter(
    [{
      path: '/',
      element: <AppLayoutWithClient client={createTestApiClient({ getResources: async () => ready(resources) })} />,
      children: [{ path: 'resources', element: <ResourceCenterPage /> }],
    }],
    { initialEntries: ['/resources'] },
  );
  return render(<RouterProvider router={router} />);
}

describe('ResourceCenterPage', () => {
  it('shows heterogeneous device capabilities without treating missing MIG as failure', async () => {
    const user = userEvent.setup();
    renderPage();

    expect(await screen.findByRole('heading', { name: '算力资源', level: 1 })).toBeInTheDocument();
    expect(screen.getByText('NVIDIA A10')).toBeInTheDocument();
    expect(screen.getAllByText('HAMi 共享').length).toBeGreaterThan(0);
    expect(screen.getAllByText('MIG 硬件切片').length).toBeGreaterThan(0);
    expect(screen.getAllByText('不适用').length).toBeGreaterThan(0);

    await user.selectOptions(screen.getByLabelText('调度方式'), 'mig');

    expect(screen.queryByText('NVIDIA A10')).not.toBeInTheDocument();
    expect(screen.getByText('NVIDIA A100 · 1g.10gb')).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('调度方式'), 'all');
    expect(screen.getByText('使用中')).toBeInTheDocument();
  });
});
