import { useState, useEffect, useRef, useCallback } from 'react';

interface SpeechRecognitionEvent {
  results: SpeechRecognitionResultList;
  resultIndex: number;
}

interface SpeechRecognitionErrorEvent {
  error: string;
}

interface SpeechRecognitionInstance extends EventTarget {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  start(): void;
  stop(): void;
  abort(): void;
  onresult: ((event: SpeechRecognitionEvent) => void) | null;
  onerror: ((event: SpeechRecognitionErrorEvent) => void) | null;
  onend: (() => void) | null;
}

declare global {
  interface Window {
    SpeechRecognition: new () => SpeechRecognitionInstance;
    webkitSpeechRecognition: new () => SpeechRecognitionInstance;
  }
}

// Natural/neural voices are prioritized — they sound human-like vs robotic SAPI voices.
// On Windows 11 Edge/Chrome these appear as "Microsoft <Name> Online (Natural)".
const PREFERRED_NATURAL_VOICES = [
  // Microsoft neural (Natural) voices — best quality on Windows/Edge/Chrome
  'microsoft aria online (natural)',
  'microsoft jenny online (natural)',
  'microsoft ana online (natural)',
  'microsoft sara online (natural)',
  // Google neural voices — available in Chrome
  'google uk english female',
  'google us english',
  // macOS high-quality voices
  'samantha',
  'karen',
  'moira',
  'tessa',
  'fiona',
  // Fallback Microsoft voices (still decent)
  'microsoft zira',
  'zira',
  'victoria',
];

function isNaturalVoice(voice: SpeechSynthesisVoice): boolean {
  const name = voice.name.toLowerCase();
  return name.includes('natural') || name.includes('neural') || name.includes('online');
}

function selectBestVoice(): SpeechSynthesisVoice | null {
  const voices = window.speechSynthesis.getVoices();
  const englishVoices = voices.filter((v) => v.lang.startsWith('en'));

  // First pass: match preferred list (natural voices listed first)
  for (const preferred of PREFERRED_NATURAL_VOICES) {
    const match = englishVoices.find((v) =>
      v.name.toLowerCase().includes(preferred)
    );
    if (match) return match;
  }

  // Second pass: any English neural/natural voice we missed
  const anyNatural = englishVoices.find(isNaturalVoice);
  if (anyNatural) return anyNatural;

  // Last resort: first English voice
  return englishVoices[0] || null;
}

function stripMarkdown(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, '') // code blocks
    .replace(/`[^`]*`/g, '')        // inline code
    .replace(/\*\*([^*]+)\*\*/g, '$1') // bold
    .replace(/\*([^*]+)\*/g, '$1')     // italic
    .replace(/^[-*•]\s+/gm, '')        // bullets
    .replace(/^#+\s+/gm, '')           // headings
    .replace(/\n{2,}/g, '. ')          // paragraph breaks to pauses
    .replace(/\n/g, ' ')
    .trim();
}

export function useVoice() {
  const [isListening, setIsListening] = useState(false);
  const [transcript, setTranscript] = useState('');
  const [isSpeaking, setIsSpeaking] = useState(false);

  const recognitionRef = useRef<SpeechRecognitionInstance | null>(null);
  const utteranceRef = useRef<SpeechSynthesisUtterance | null>(null);

  const SpeechRecognitionCtor =
    typeof window !== 'undefined'
      ? window.SpeechRecognition || window.webkitSpeechRecognition
      : null;

  const isSTTSupported = !!SpeechRecognitionCtor;
  const isTTSSupported =
    typeof window !== 'undefined' && 'speechSynthesis' in window;

  const startListening = useCallback(() => {
    if (!SpeechRecognitionCtor || isListening) return;

    const recognition = new SpeechRecognitionCtor();
    recognition.continuous = false;
    recognition.interimResults = true;
    recognition.lang = 'en-US';

    recognition.onresult = (event: SpeechRecognitionEvent) => {
      let finalTranscript = '';
      let interimTranscript = '';

      for (let i = event.resultIndex; i < event.results.length; i++) {
        const result = event.results[i];
        if (result.isFinal) {
          finalTranscript += result[0].transcript;
        } else {
          interimTranscript += result[0].transcript;
        }
      }

      setTranscript(finalTranscript || interimTranscript);
    };

    recognition.onerror = (event: SpeechRecognitionErrorEvent) => {
      if (event.error !== 'no-speech' && event.error !== 'aborted') {
        console.warn('Speech recognition error:', event.error);
      }
      setIsListening(false);
    };

    recognition.onend = () => {
      setIsListening(false);
    };

    recognitionRef.current = recognition;
    setTranscript('');
    setIsListening(true);
    recognition.start();
  }, [SpeechRecognitionCtor, isListening]);

  const stopListening = useCallback(() => {
    if (recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
    }
    setIsListening(false);
  }, []);

  const speak = useCallback(
    (text: string) => {
      if (!isTTSSupported || !text.trim()) return;

      // Cancel any ongoing speech
      window.speechSynthesis.cancel();

      const cleaned = stripMarkdown(text);
      const utterance = new SpeechSynthesisUtterance(cleaned);

      const voice = selectBestVoice();
      if (voice) utterance.voice = voice;

      // Slightly slower rate + natural pitch for conversational tone
      utterance.rate = 0.95;
      utterance.pitch = 1.0;

      utterance.onstart = () => setIsSpeaking(true);
      utterance.onend = () => setIsSpeaking(false);
      utterance.onerror = () => setIsSpeaking(false);

      utteranceRef.current = utterance;
      window.speechSynthesis.speak(utterance);
    },
    [isTTSSupported]
  );

  const stopSpeaking = useCallback(() => {
    if (isTTSSupported) {
      window.speechSynthesis.cancel();
    }
    setIsSpeaking(false);
  }, [isTTSSupported]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.abort();
      }
      if (isTTSSupported) {
        window.speechSynthesis.cancel();
      }
    };
  }, [isTTSSupported]);

  // Load voices (Chrome loads them async)
  useEffect(() => {
    if (!isTTSSupported) return;
    const handleVoicesChanged = () => {
      // Voices loaded — component will pick them up on next speak()
    };
    window.speechSynthesis.addEventListener('voiceschanged', handleVoicesChanged);
    return () => {
      window.speechSynthesis.removeEventListener('voiceschanged', handleVoicesChanged);
    };
  }, [isTTSSupported]);

  return {
    // STT
    isListening,
    isSTTSupported,
    transcript,
    startListening,
    stopListening,
    // TTS
    isSpeaking,
    isTTSSupported,
    speak,
    stopSpeaking,
  };
}
