/**
 * Detect whether an AI message contains actionable insights worth reading aloud.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isInsightMessage(
  content: string,
  metadata?: any
): boolean {
  if (!content || typeof content !== 'string') return false;

  // Primary indicator — Notable section from backend system prompt
  if (/\*\*Notable:\*\*/i.test(content)) return true;

  // Recommendation keywords
  if (/\b(recommend|should consider|prioritize|suggest)\b/i.test(content)) return true;

  // Analytical patterns only for substantial messages
  if (
    content.length > 200 &&
    /\b(trend shows?|analysis indicates?|increased by \d+%|decreased by \d+%|significant(ly)?|compared to)\b/i.test(content)
  ) {
    return true;
  }

  // If metadata flags it as insight type
  if (metadata?.response_type === 'insight') return true;

  return false;
}

/**
 * Extract the insight-relevant portion of a message for TTS.
 * Prefers the Notable section; falls back to the full content.
 */
export function extractInsightText(content: string): string {
  if (!content || typeof content !== 'string') return '';

  // Try to extract the Notable section
  const notableMatch = content.match(/\*\*Notable:\*\*\s*([\s\S]*?)$/i);
  if (notableMatch) {
    return notableMatch[1].trim();
  }

  // Try to extract recommendation sentences
  const sentences = content.split(/(?<=[.!?])\s+/);
  const insightSentences = sentences.filter((s) =>
    /\b(recommend|should consider|prioritize|trend shows?|analysis indicates?|increased by|decreased by|suggest)\b/i.test(s)
  );

  if (insightSentences.length > 0) {
    return insightSentences.join(' ');
  }

  // Fallback: return full content (stripMarkdown will be handled by useVoice)
  return content;
}
