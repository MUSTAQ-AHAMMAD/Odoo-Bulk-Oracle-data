import { Link } from 'react-router-dom';

const links = [
  ['Dashboard', '/dashboard'],
  ['Upload', '/upload'],
  ['Templates', '/templates'],
  ['Jobs', '/jobs']
] as const;

export const Sidebar = (): JSX.Element => (
  <aside className="w-56 border-r p-4">
    <nav className="space-y-2">
      {links.map(([label, href]) => (
        <Link key={href} to={href} className="block rounded px-3 py-2 hover:bg-gray-200 dark:hover:bg-gray-700">
          {label}
        </Link>
      ))}
    </nav>
  </aside>
);
