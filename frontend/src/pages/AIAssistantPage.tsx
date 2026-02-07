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
    loadConversations,
    loadConversation,
    startNewConversation,
    deleteConversation,
    clearAllConversations,
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

  // Auto-scroll to latest message/chart/table
  useEffect(() => {
    // Check if the last message has data (charts/tables take longer to render)
    const lastMessage = currentConversation?.messages[currentConversation.messages.length - 1];
    const hasData = lastMessage?.metadata?.data && lastMessage.metadata.data.length > 0;

    // Longer delay for messages with charts/tables
    const delay = hasData ? 300 : 100;

    const timer = setTimeout(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }, delay);
    return () => clearTimeout(timer);
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

  const handleClearHistory = async () => {
    if (conversations.length === 0) return;
    if (confirm('Are you sure you want to clear all chat history?')) {
      await clearAllConversations();
      navigate('/assistant');
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

        {conversations.length > 0 && (
          <div className="p-3 border-t border-white/10">
            <button
              onClick={handleClearHistory}
              className="w-full flex items-center justify-center space-x-2 px-4 py-2 rounded-xl text-white/50 hover:text-red-400 hover:bg-red-500/10 transition-all text-sm"
            >
              <Trash2 className="h-4 w-4" />
              <span>Clear History</span>
            </button>
          </div>
        )}
      </div>

      {/* Main chat area */}
      <div className="flex-1 glass-panel flex flex-col overflow-hidden">
        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6 glass-scrollbar">
          {currentConversation?.messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full">
              <div className="relative mb-6">
                <img
                  src="/leabot-avatar.png"
                  alt="Lea AI Assistant"
                  className="relative h-[640px] w-auto object-contain"
                  style={{ mixBlendMode: 'lighten' }}
                />
              </div>
              <h2 className="text-2xl font-bold text-white mb-2">
                I'm <span className="text-brand-orange italic">Lea</span> your AI assistant
              </h2>
              <p className="text-white/50 text-center max-w-md mb-8">
                Ask me anything about your fleet data. I can help you analyze metrics,
                generate reports, and provide intelligent insights.
              </p>

              <div className="grid gap-3 sm:grid-cols-2 max-w-xl w-full">
                {[
                  { icon: Code, text: 'Show me the total number of vehicles by status' },
                  { icon: Lightbulb, text: 'Provide the total number of renewals for the next 6 months' },
                  { icon: Code, text: 'Show contracts expiring in the next 30 days' },
                  { icon: Lightbulb, text: 'Generate insights about maintenance costs' },
                ].map((suggestion, index) => (
                  <button
                    key={index}
                    onClick={() => handleSuggestionClick(suggestion.text)}
                    className="glass-card flex items-start space-x-3 p-4 text-left group hover:scale-[1.02] transition-all duration-200"
                    style={{ animationDelay: `${index * 100}ms` }}
                  >
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg transition-colors" style={{ backgroundColor: 'rgba(169, 201, 14, 0.2)' }}>
                      <suggestion.icon className="h-4 w-4" style={{ color: '#a9c90e' }} />
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
                  className="animate-fade-in"
                  style={{ animationDelay: `${index * 50}ms` }}
                >
                  {/* Message bubble */}
                  <div className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`flex items-start space-x-3 max-w-[80%] ${message.role === 'user' ? 'flex-row-reverse space-x-reverse' : ''}`}>
                      {message.role === 'user' ? (
                        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-brand-orange to-brand-amber">
                          <User className="h-4 w-4 text-white" />
                        </div>
                      ) : (
                        <img
                          src="/bot-avatar.png"
                          alt="Lea"
                          className="h-9 w-9 shrink-0 rounded-xl object-cover"
                        />
                      )}
                      <div
                        className={`rounded-2xl px-5 py-3 ${
                          message.role === 'user'
                            ? 'bg-gradient-to-r from-brand-orange to-brand-orange-light text-white'
                            : 'glass border-brand-cyan/20'
                        }`}
                      >
                        <div className="text-sm whitespace-pre-wrap text-white/90">
                          {typeof message.content === 'string'
                            ? (() => {
                                // Split content to separate the Notable section
                                const notableMatch = message.content.match(/\*\*Notable:\*\*\s*([\s\S]*?)$/i);
                                if (notableMatch) {
                                  const beforeNotable = message.content.substring(0, message.content.indexOf('**Notable:**'));
                                  const notableText = notableMatch[1];
                                  return (
                                    <>
                                      <span>{beforeNotable}</span>
                                      <div
                                        className="mt-3 p-3 rounded-xl border"
                                        style={{
                                          backgroundColor: 'rgba(169, 201, 14, 0.1)',
                                          borderColor: 'rgba(169, 201, 14, 0.3)'
                                        }}
                                      >
                                        <span style={{ color: '#a9c90e', fontWeight: 'bold' }}>Notable: </span>
                                        <span style={{ color: '#a9c90e' }}>{notableText.replace(/\*\*/g, '')}</span>
                                      </div>
                                    </>
                                  );
                                }
                                return message.content;
                              })()
                            : JSON.stringify(message.content)}
                        </div>
                        {message.metadata && typeof message.metadata === 'object' && message.metadata.sql && (
                          <pre className="mt-3 p-3 bg-black/30 rounded-xl text-xs overflow-x-auto border border-white/10">
                            <code className="text-brand-cyan">{message.metadata.sql}</code>
                          </pre>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Chart visualization - full width below message */}
                  {message.role === 'assistant' && message.metadata?.data && message.metadata.data.length > 0 && (() => {
                    const chartConfig = message.metadata.chart_config ?? detectChartConfig(message.metadata.data);
                    return chartConfig ? (
                      <div className="mt-4 ml-12">
                        <AIResponseChart data={message.metadata.data} chartConfig={chartConfig} />
                      </div>
                    ) : null;
                  })()}

                  {/* Data table - full width below chart */}
                  {message.role === 'assistant' && message.metadata?.data && message.metadata.data.length > 0 && (
                    <div className="mt-3 ml-12 max-h-64 overflow-auto glass-scrollbar">
                      <div className="glass rounded-xl overflow-hidden">
                        <table className="w-full text-xs">
                          <thead className="bg-white/5 sticky top-0">
                            <tr>
                              {Object.keys(message.metadata.data[0]).map((key) => (
                                <th key={key} className="px-4 py-2.5 text-left font-semibold text-white/70 uppercase tracking-wide">
                                  {key.replace(/_/g, ' ')}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-white/5">
                            {message.metadata.data.slice(0, 15).map((row, i) => (
                              <tr key={i} className="hover:bg-white/5 transition-colors">
                                {Object.values(row).map((val, j) => (
                                  <td key={j} className="px-4 py-2 text-white/70">
                                    {typeof val === 'number' ? val.toLocaleString() : String(val ?? '-')}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {message.metadata.data.length > 15 && (
                        <p className="text-xs text-white/40 mt-2 text-center">
                          Showing 15 of {message.metadata.data.length} rows
                        </p>
                      )}
                    </div>
                  )}
                </div>
              ))}

              {isSending && (
                <div className="flex justify-start animate-fade-in">
                  <div className="flex items-start space-x-3">
                    <img
                      src="/bot-avatar.png"
                      alt="Lea"
                      className="h-9 w-9 shrink-0 rounded-xl object-cover"
                    />
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
                className="glass-button text-xs px-4 py-2 rounded-full transition-colors hover:opacity-80"
                style={{ color: '#a9c90e', borderColor: 'rgba(169, 201, 14, 0.3)' }}
              >
                {suggestion}
              </button>
            ))}
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
