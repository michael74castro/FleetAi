import { useEffect, useRef, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Send, Plus, Trash2, MessageSquare, Sparkles, Code, Lightbulb, Bot, User } from 'lucide-react';
import { useAIStore } from '@/store/aiStore';
import AIResponseChart from '@/components/ai/AIResponseChart';
import { detectChartConfig } from '@/utils/chartDetection';

export default function AIAssistantPage() {
  const { conversationId } = useParams();
  const navigate = useNavigate();
  const {
    conversations,
    currentConversation,
    isSending,
    suggestions,
    lastResponse,
    loadConversations,
    loadConversation,
    startNewConversation,
    deleteConversation,
    sendMessage,
  } = useAIStore();

  const [input, setInput] = useState('');
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    loadConversations();
    if (conversationId) {
      loadConversation(conversationId);
    } else {
      startNewConversation();
    }
  }, [conversationId, loadConversations, loadConversation, startNewConversation]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [currentConversation?.messages]);

  const handleSend = async () => {
    if (!input.trim() || isSending) return;

    const message = input.trim();
    setInput('');

    try {
      const response = await sendMessage(message);
      if (!conversationId && response.conversation_id) {
        navigate(`/assistant/${response.conversation_id}`, { replace: true });
      }
    } catch (error) {
      console.error('Failed to send message:', error);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    setInput(suggestion);
    inputRef.current?.focus();
  };

  const handleNewChat = () => {
    startNewConversation();
    navigate('/assistant');
  };

  const handleDeleteConversation = async (id: string | number, e: React.MouseEvent) => {
    e.stopPropagation();
    if (confirm('Are you sure you want to delete this conversation?')) {
      await deleteConversation(id);
      if (conversationId === String(id)) {
        navigate('/assistant');
      }
    }
  };

  return (
    <div className="flex h-[calc(100vh-8rem)] gap-6 animate-fade-in">
      {/* Sidebar */}
      <div className="w-72 glass-panel flex flex-col">
        <div className="p-4 border-b border-white/10">
          <button
            onClick={handleNewChat}
            className="glass-button-primary w-full flex items-center justify-center space-x-2 px-4 py-3 rounded-xl text-white font-semibold group"
          >
            <Plus className="h-5 w-5 group-hover:rotate-90 transition-transform duration-300" />
            <span>New Chat</span>
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-3 space-y-1 glass-scrollbar">
          {conversations.map((conv) => (
            <div
              key={conv.conversation_id}
              onClick={() => navigate(`/assistant/${conv.conversation_id}`)}
              className={`group flex items-center justify-between rounded-xl px-3 py-3 cursor-pointer transition-all duration-200 ${
                conversationId === String(conv.conversation_id)
                  ? 'glass-light border-brand-orange/30'
                  : 'hover:bg-white/5'
              }`}
            >
              <div className="flex items-center space-x-3 overflow-hidden">
                <div className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${
                  conversationId === String(conv.conversation_id)
                    ? 'bg-brand-orange/20 text-brand-orange'
                    : 'bg-white/10 text-white/50'
                }`}>
                  <MessageSquare className="h-4 w-4" />
                </div>
                <span className="text-sm truncate text-white/80">{conv.title || 'New Chat'}</span>
              </div>
              <button
                onClick={(e) => handleDeleteConversation(conv.conversation_id, e)}
                className="opacity-0 group-hover:opacity-100 p-1.5 hover:bg-red-500/20 rounded-lg text-white/40 hover:text-red-400 transition-all"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Main chat area */}
      <div className="flex-1 glass-panel flex flex-col overflow-hidden">
        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6 glass-scrollbar">
          {currentConversation?.messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full">
              <div className="relative mb-6">
                <div className="absolute inset-0 bg-brand-orange rounded-full blur-3xl opacity-30 animate-pulse-glow" />
                <div className="relative flex h-24 w-24 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-orange/20 to-brand-amber/20 border border-brand-orange/30">
                  <Sparkles className="h-12 w-12 text-brand-orange" />
                </div>
              </div>
              <h2 className="text-2xl font-bold text-white mb-2">MyFleet AI Assistant</h2>
              <p className="text-white/50 text-center max-w-md mb-8">
                Ask me anything about your fleet data. I can help you analyze metrics,
                generate reports, and provide intelligent insights.
              </p>

              <div className="grid gap-3 sm:grid-cols-2 max-w-xl w-full">
                {[
                  { icon: Code, text: 'Show me the total number of vehicles by status' },
                  { icon: Lightbulb, text: 'What are the top 5 customers by fuel consumption?' },
                  { icon: Code, text: 'Show contracts expiring in the next 30 days' },
                  { icon: Lightbulb, text: 'Generate insights about maintenance costs' },
                ].map((suggestion, index) => (
                  <button
                    key={index}
                    onClick={() => handleSuggestionClick(suggestion.text)}
                    className="glass-card flex items-start space-x-3 p-4 text-left group hover:scale-[1.02] transition-all duration-200"
                    style={{ animationDelay: `${index * 100}ms` }}
                  >
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-brand-cyan/20 group-hover:bg-brand-cyan/30 transition-colors">
                      <suggestion.icon className="h-4 w-4 text-brand-cyan" />
                    </div>
                    <span className="text-sm text-white/70 group-hover:text-white/90 transition-colors">{suggestion.text}</span>
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <>
              {currentConversation?.messages.map((message, index) => (
                <div
                  key={message.message_id}
                  className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'} animate-fade-in`}
                  style={{ animationDelay: `${index * 50}ms` }}
                >
                  <div className={`flex items-start space-x-3 max-w-[80%] ${message.role === 'user' ? 'flex-row-reverse space-x-reverse' : ''}`}>
                    <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-xl ${
                      message.role === 'user'
                        ? 'bg-gradient-to-br from-brand-orange to-brand-amber'
                        : 'bg-gradient-to-br from-brand-cyan/30 to-brand-cyan/20 border border-brand-cyan/30'
                    }`}>
                      {message.role === 'user' ? (
                        <User className="h-4 w-4 text-white" />
                      ) : (
                        <Bot className="h-4 w-4 text-brand-cyan" />
                      )}
                    </div>
                    <div
                      className={`rounded-2xl px-5 py-3 ${
                        message.role === 'user'
                          ? 'bg-gradient-to-r from-brand-orange to-brand-orange-light text-white'
                          : 'glass border-brand-cyan/20'
                      }`}
                    >
                      <p className="text-sm whitespace-pre-wrap text-white/90">
                        {typeof message.content === 'string' ? message.content : JSON.stringify(message.content)}
                      </p>
                      {message.metadata && typeof message.metadata === 'object' && message.metadata.sql && (
                        <pre className="mt-3 p-3 bg-black/30 rounded-xl text-xs overflow-x-auto border border-white/10">
                          <code className="text-brand-cyan">{message.metadata.sql}</code>
                        </pre>
                      )}
                    </div>
                  </div>
                </div>
              ))}

              {isSending && (
                <div className="flex justify-start animate-fade-in">
                  <div className="flex items-start space-x-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-brand-cyan/30 to-brand-cyan/20 border border-brand-cyan/30">
                      <Bot className="h-4 w-4 text-brand-cyan" />
                    </div>
                    <div className="glass border-brand-cyan/20 rounded-2xl px-5 py-4">
                      <div className="flex space-x-1.5">
                        <div className="w-2 h-2 bg-brand-cyan rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                        <div className="w-2 h-2 bg-brand-cyan rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                        <div className="w-2 h-2 bg-brand-cyan rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                      </div>
                    </div>
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </>
          )}
        </div>

        {/* Suggestions */}
        {suggestions.length > 0 && (
          <div className="px-6 py-3 border-t border-white/10 flex flex-wrap gap-2">
            {suggestions.map((suggestion, index) => (
              <button
                key={index}
                onClick={() => handleSuggestionClick(suggestion)}
                className="glass-button text-xs px-4 py-2 rounded-full text-white/70 hover:text-white transition-colors"
              >
                {suggestion}
              </button>
            ))}
          </div>
        )}

        {/* Chart visualization if available */}
        {lastResponse?.data && lastResponse.data.length > 0 && (() => {
          const chartConfig = lastResponse.chart_config ?? detectChartConfig(lastResponse.data);
          return chartConfig ? (
            <div className="px-6 py-3 border-t border-white/10">
              <AIResponseChart data={lastResponse.data} chartConfig={chartConfig} />
            </div>
          ) : null;
        })()}

        {/* Data table if available */}
        {lastResponse?.data && lastResponse.data.length > 0 && (
          <div className="px-6 py-4 border-t border-white/10 max-h-52 overflow-auto glass-scrollbar">
            <div className="glass rounded-xl overflow-hidden">
              <table className="w-full text-xs">
                <thead className="bg-white/5 sticky top-0">
                  <tr>
                    {Object.keys(lastResponse.data[0]).map((key) => (
                      <th key={key} className="px-4 py-3 text-left font-semibold text-white/70 uppercase tracking-wide">
                        {key}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {lastResponse.data.slice(0, 10).map((row, i) => (
                    <tr key={i} className="hover:bg-white/5 transition-colors">
                      {Object.values(row).map((val, j) => (
                        <td key={j} className="px-4 py-2.5 text-white/70">
                          {String(val ?? '-')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {lastResponse.data.length > 10 && (
              <p className="text-xs text-white/40 mt-2 text-center">
                Showing 10 of {lastResponse.data.length} rows
              </p>
            )}
          </div>
        )}

        {/* Input */}
        <div className="border-t border-white/10 p-4">
          <div className="flex items-end space-x-3">
            <div className="flex-1 relative">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask me anything about your fleet data..."
                rows={1}
                className="w-full glass-input px-5 py-4 pr-14 text-sm text-white resize-none"
                style={{ minHeight: '56px', maxHeight: '160px' }}
              />
            </div>
            <button
              onClick={handleSend}
              disabled={!input.trim() || isSending}
              className="glass-button-primary flex items-center justify-center p-4 rounded-xl disabled:opacity-50 disabled:cursor-not-allowed group"
            >
              <Send className="h-5 w-5 text-white group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
