import { useTheme } from '../../context/ThemeContext';
import { useAuth } from '../../context/AuthContext';

export const Navbar = (): JSX.Element => {
  const { darkMode, toggleTheme } = useTheme();
  const { logout } = useAuth();

  return (
    <header className="flex items-center justify-between border-b p-4">
      <h1 className="text-lg font-semibold">CRM Bulk Mapper</h1>
      <div className="flex gap-2">
        <button className="rounded border px-3 py-1" onClick={toggleTheme}>
          {darkMode ? 'Light' : 'Dark'}
        </button>
        <button className="rounded bg-red-600 px-3 py-1 text-white" onClick={logout}>
          Logout
        </button>
      </div>
    </header>
  );
};
