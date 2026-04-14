import { createContext, useContext, useMemo, useState, type ReactNode } from 'react';

interface AuthContextValue {
  token: string | null;
  isAuthenticated: boolean;
  setToken: (token: string | null) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }): JSX.Element => {
  const [token, setTokenState] = useState<string | null>(() => localStorage.getItem('token'));

  const setToken = (value: string | null): void => {
    if (value) localStorage.setItem('token', value);
    else localStorage.removeItem('token');
    setTokenState(value);
  };

  const value = useMemo<AuthContextValue>(
    () => ({ token, isAuthenticated: Boolean(token), setToken, logout: () => setToken(null) }),
    [token]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = (): AuthContextValue => {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
};
