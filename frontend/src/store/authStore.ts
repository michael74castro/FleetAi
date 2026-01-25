import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { User } from '@/types';

interface AuthState {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;

  // Actions
  setUser: (user: User | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  logout: () => void;

  // Helpers
  hasPermission: (permission: string) => boolean;
  hasRole: (role: string) => boolean;
  canAccessCustomer: (customerId: string) => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      isAuthenticated: false,
      isLoading: true,
      error: null,

      setUser: (user) =>
        set({
          user,
          isAuthenticated: !!user,
          isLoading: false,
          error: null,
        }),

      setLoading: (loading) => set({ isLoading: loading }),

      setError: (error) => set({ error, isLoading: false }),

      logout: () =>
        set({
          user: null,
          isAuthenticated: false,
          isLoading: false,
          error: null,
        }),

      hasPermission: (permission) => {
        const { user } = get();
        if (!user) return false;
        return user.permissions.includes(permission);
      },

      hasRole: (role) => {
        const { user } = get();
        if (!user) return false;
        return user.role_name === role;
      },

      canAccessCustomer: (customerId) => {
        const { user } = get();
        if (!user) return false;
        // null customer_ids means access to all
        if (user.customer_ids === null) return true;
        return user.customer_ids.includes(customerId);
      },
    }),
    {
      name: 'fleetai-auth',
      partialize: (state) => ({ user: state.user, isAuthenticated: state.isAuthenticated }),
    }
  )
);
