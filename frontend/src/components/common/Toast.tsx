import type { ToastItem } from '../../hooks/useToast';

const color: Record<ToastItem['type'], string> = {
  success: 'bg-green-600',
  error: 'bg-red-600',
  info: 'bg-blue-600'
};

export const Toast = ({ items }: { items: ToastItem[] }): JSX.Element => (
  <div className="fixed right-4 top-4 z-50 space-y-2">
    {items.map((item) => (
      <div key={item.id} className={`rounded px-4 py-2 text-sm text-white ${color[item.type]}`}>
        {item.message}
      </div>
    ))}
  </div>
);
