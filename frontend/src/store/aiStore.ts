import { create } from 'zustand';
import type { Conversation, Message, ChatResponse, InsightResult, WidgetSuggestion } from '@/types';
import { api } from '@/services/api';

interface AIState {
  // Conversations
  conversations: Conversation[];
  currentConversation: Conversation | null;
  isLoading: boolean;
  isSending: boolean;
  error: string | null;

  // Current response data
  lastResponse: ChatResponse | null;
  suggestions: string[];

  // Actions
  loadConversations: () => Promise<void>;
  loadConversation: (id: string | number) => Promise<void>;
  startNewConversation: () => void;
  deleteConversation: (id: string | number) => Promise<void>;

  // Chat
  sendMessage: (message: string) => Promise<ChatResponse>;
  clearMessages: () => void;

  // AI Features
  generateSQL: (query: string, execute?: boolean) => Promise<{ sql: string; results?: unknown[] }>;
  generateInsights: (dataset: string, metric?: string) => Promise<InsightResult>;
  suggestWidgets: (dataset: string, description?: string) => Promise<WidgetSuggestion[]>;

  // Error handling
  setError: (error: string | null) => void;
}

export const useAIStore = create<AIState>((set, get) => ({
  conversations: [],
  currentConversation: null,
  isLoading: false,
  isSending: false,
  error: null,
  lastResponse: null,
  suggestions: [],

  loadConversations: async () => {
    set({ isLoading: true, error: null });
    try {
      const response = await api.getConversations({ page_size: 50 });
      set({ conversations: response.items, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to load conversations', isLoading: false });
    }
  },

  loadConversation: async (id) => {
    set({ isLoading: true, error: null });
    try {
      const conversation = await api.getConversation(id);
      set({ currentConversation: conversation, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to load conversation', isLoading: false });
    }
  },

  startNewConversation: () => {
    set({
      currentConversation: {
        conversation_id: 0,
        title: 'New Conversation',
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        messages: [],
      },
      lastResponse: null,
      suggestions: [],
    });
  },

  deleteConversation: async (id) => {
    await api.deleteConversation(id);
    set((state) => ({
      conversations: state.conversations.filter((c) => c.conversation_id !== id),
      currentConversation:
        state.currentConversation?.conversation_id === id ? null : state.currentConversation,
    }));
  },

  sendMessage: async (message) => {
    const { currentConversation } = get();

    // Add user message optimistically
    const userMessage: Message = {
      message_id: Date.now(),
      role: 'user',
      content: message,
      created_at: new Date().toISOString(),
    };

    set((state) => ({
      currentConversation: state.currentConversation
        ? {
            ...state.currentConversation,
            messages: [...state.currentConversation.messages, userMessage],
          }
        : {
            conversation_id: 0,
            title: message.slice(0, 50),
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            messages: [userMessage],
          },
      isSending: true,
      error: null,
    }));

    try {
      const convId = currentConversation?.conversation_id;
      const response = await api.sendChatMessage(
        message,
        convId && convId !== 0 && convId !== '0' ? convId : undefined
      );

      // Add assistant message - response.message is a MessageResponse object
      const msg = response.message;
      const assistantMessage: Message = {
        message_id: typeof msg === 'object' ? msg.message_id : Date.now(),
        role: 'assistant',
        content: typeof msg === 'object' ? msg.content : String(msg),
        metadata: typeof msg === 'object' ? msg.metadata : undefined,
        created_at: typeof msg === 'object' && msg.created_at ? msg.created_at : new Date().toISOString(),
      };

      set((state) => ({
        currentConversation: state.currentConversation
          ? {
              ...state.currentConversation,
              conversation_id: response.conversation_id || state.currentConversation.conversation_id,
              messages: [...state.currentConversation.messages, assistantMessage],
            }
          : null,
        lastResponse: response,
        suggestions: response.suggestions || [],
        isSending: false,
      }));

      // Update conversations list if new
      if (!currentConversation || !currentConversation.conversation_id || currentConversation.conversation_id === 0) {
        get().loadConversations();
      }

      return response;
    } catch (error) {
      set({ error: 'Failed to send message', isSending: false });
      throw error;
    }
  },

  clearMessages: () => {
    set((state) => ({
      currentConversation: state.currentConversation
        ? { ...state.currentConversation, messages: [] }
        : null,
      lastResponse: null,
      suggestions: [],
    }));
  },

  generateSQL: async (query, execute = false) => {
    set({ isLoading: true, error: null });
    try {
      const result = await api.generateSQL(query, execute);
      set({ isLoading: false });
      return result;
    } catch (error) {
      set({ error: 'Failed to generate SQL', isLoading: false });
      throw error;
    }
  },

  generateInsights: async (dataset, metric) => {
    set({ isLoading: true, error: null });
    try {
      const result = await api.generateInsights({ dataset, metric });
      set({ isLoading: false });
      return result;
    } catch (error) {
      set({ error: 'Failed to generate insights', isLoading: false });
      throw error;
    }
  },

  suggestWidgets: async (dataset, description) => {
    set({ isLoading: true, error: null });
    try {
      const result = await api.suggestDashboardWidgets(dataset, description);
      set({ isLoading: false });
      return result.suggestions || [];
    } catch (error) {
      set({ error: 'Failed to get widget suggestions', isLoading: false });
      throw error;
    }
  },

  setError: (error) => set({ error }),
}));
