import React from 'react';
import {
  AbsoluteFill,
  Img,
  Sequence,
  interpolate,
  spring,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from 'remotion';

// ── Scene definitions ──────────────────────────────────────────────────────
// Each entry: screenshot filename, title, subtitle, duration in frames (30fps)
const SCENES = [
  { img: '01_welcome.png',          title: 'CloudEagle',                       sub: 'AI-powered Custom Connector Builder',          dur: 75  },
  { img: '02_connector_grid.png',   title: 'Connect any API',                  sub: 'Built-in connectors or build custom from any docs URL', dur: 60 },
  { img: '03_build_started.png',    title: 'One-click build',                  sub: 'Describe the API — the AI takes it from there', dur: 60 },
  { img: '04_pipeline_open.png',    title: 'Live build pipeline',              sub: 'Watch every step in real time',                dur: 60  },
  { img: '05_pipeline_mid.png',     title: 'AI Manifest Fill',                 sub: 'Claude reads the docs and fills every field with a citation', dur: 75 },
  { img: '06_pipeline_done.png',    title: '4-layer validation',               sub: 'Schema · Secret scan · Live API probe · Contract test', dur: 75 },
  { img: '07_fields_extracted.png', title: 'Grounded field extraction',        sub: 'Every value traced back to its source — no hallucinations', dur: 75 },
  { img: '08_connectors_page.png',  title: 'Connector registry',               sub: 'Versioned manifests with full audit trail',    dur: 60  },
  { img: '09_configure_settings.png', title: 'Human review & editing',         sub: 'Tweak auth, base URL, rate limits before going live', dur: 60 },
  { img: '10_configure_streams.png',  title: 'Stream configuration',           sub: 'Add, remove or edit streams at any time',      dur: 60  },
  { img: '11_sync_tab.png',         title: 'One-click data sync',              sub: 'Full / incremental modes with cursor checkpointing', dur: 55 },
  { img: '12_sync_running.png',     title: 'Live sync progress',               sub: 'Auth, pagination and retries handled automatically', dur: 60 },
  { img: '13_sync_done.png',        title: 'Sync complete',                    sub: 'Records written to destination in seconds',   dur: 60  },
  { img: '14_data_preview.png',     title: 'Data preview',                     sub: 'Inspect synced records directly in the UI',    dur: 60  },
  { img: '15_observability.png',    title: 'Full observability',               sub: 'Sync history, checkpoints and metrics in one view', dur: 75 },
];

// ── Caption bar ────────────────────────────────────────────────────────────
function Caption({ title, sub }) {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const opacity = spring({ frame, fps, config: { damping: 18, stiffness: 80 } });
  const y       = interpolate(opacity, [0, 1], [20, 0]);

  return (
    <div style={{
      position:      'absolute',
      bottom:        0,
      left:          0,
      right:         0,
      background:    'linear-gradient(transparent, rgba(0,0,0,0.78))',
      padding:       '48px 48px 36px',
      transform:     `translateY(${y}px)`,
      opacity,
    }}>
      <div style={{ fontFamily: 'Inter, system-ui, sans-serif', color: '#fff' }}>
        <div style={{ fontSize: 28, fontWeight: 700, letterSpacing: '-0.3px', marginBottom: 6 }}>
          {title}
        </div>
        <div style={{ fontSize: 16, fontWeight: 400, color: 'rgba(255,255,255,0.78)', lineHeight: 1.4 }}>
          {sub}
        </div>
      </div>
    </div>
  );
}

// ── Logo badge ─────────────────────────────────────────────────────────────
function Badge() {
  return (
    <div style={{
      position:     'absolute',
      top:          24,
      left:         28,
      background:   'rgba(37,99,235,0.92)',
      backdropFilter: 'blur(8px)',
      borderRadius: 8,
      padding:      '6px 14px',
      fontFamily:   'Inter, system-ui, sans-serif',
      fontSize:     13,
      fontWeight:   700,
      color:        '#fff',
      letterSpacing: '0.02em',
    }}>
      CloudEagle
    </div>
  );
}

// ── Single scene ───────────────────────────────────────────────────────────
function Scene({ img, title, sub, totalFrames }) {
  const frame = useCurrentFrame();

  // Fade in first 12 frames, fade out last 12 frames
  const fadeIn  = Math.min(frame / 12, 1);
  const fadeOut = Math.min((totalFrames - frame) / 12, 1);
  const opacity = Math.min(fadeIn, fadeOut);

  // Subtle Ken-Burns zoom
  const scale = interpolate(frame, [0, totalFrames], [1, 1.04]);

  return (
    <AbsoluteFill style={{ background: '#0f172a', opacity }}>
      <AbsoluteFill style={{ transform: `scale(${scale})`, transformOrigin: 'center center' }}>
        <Img
          src={staticFile(`frames/${img}`)}
          style={{ width: '100%', height: '100%', objectFit: 'cover' }}
        />
      </AbsoluteFill>
      <Badge />
      <Caption title={title} sub={sub} />
    </AbsoluteFill>
  );
}

// ── Main composition ───────────────────────────────────────────────────────
export function DemoVideo() {
  let offset = 0;
  return (
    <AbsoluteFill style={{ background: '#0f172a' }}>
      {SCENES.map((scene, i) => {
        const from = offset;
        offset += scene.dur;
        return (
          <Sequence key={i} from={from} durationInFrames={scene.dur}>
            <Scene {...scene} totalFrames={scene.dur} />
          </Sequence>
        );
      })}
    </AbsoluteFill>
  );
}

// Total frame count export (used by Root)
export const TOTAL_FRAMES = SCENES.reduce((s, sc) => s + sc.dur, 0);
