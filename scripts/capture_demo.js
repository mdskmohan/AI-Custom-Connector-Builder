/**
 * capture_demo.js
 *
 * Playwright script that drives the CloudEagle app end-to-end and captures
 * screenshots at every key moment. The screenshots are saved to
 * demo-video/public/frames/ and consumed by the Remotion video project.
 *
 * Prerequisites:
 *   npm install playwright
 *   npx playwright install chromium
 *
 * Usage (app must be running on localhost:8000):
 *   node scripts/capture_demo.js
 */

const { chromium } = require('playwright');
const path = require('path');
const fs   = require('fs');

const APP_URL   = 'http://localhost:8000';
const FRAMES_DIR = path.join(__dirname, '../demo-video/public/frames');

const VIEWPORT = { width: 1280, height: 720 };

// ── helpers ────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function shot(page, name, label) {
  const file = path.join(FRAMES_DIR, `${name}.png`);
  await page.screenshot({ path: file });
  console.log(`  captured  ${name}.png  —  ${label}`);
}

// Poll until a JS expression evaluates to true, or timeout
async function waitFor(page, expr, timeout = 60000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const ok = await page.evaluate(expr).catch(() => false);
    if (ok) return true;
    await sleep(500);
  }
  return false;
}

// ── main ───────────────────────────────────────────────────────────────────

async function main() {
  fs.mkdirSync(FRAMES_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: false, slowMo: 40 });
  const ctx     = await browser.newContext({ viewport: VIEWPORT });
  const page    = await ctx.newPage();

  console.log('\nCloudEagle demo capture starting...\n');

  // ── 01  Welcome / home ────────────────────────────────────────────────────
  await page.goto(APP_URL, { waitUntil: 'networkidle' });
  await sleep(1200);
  await shot(page, '01_welcome', 'Welcome screen');

  // ── 02  Connector grid ────────────────────────────────────────────────────
  // Scroll the connector grid into view
  await page.evaluate(() => {
    const el = document.getElementById('conn-grid-items');
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  });
  await sleep(800);
  await shot(page, '02_connector_grid', 'Built-in connector grid');

  // ── 03  Click GitHub (needsAuth: false → pipeline starts immediately) ─────
  await page.evaluate(() => handleConnector('github'));
  await sleep(1000);
  await shot(page, '03_build_started', 'Build started — chat view');

  // Open the artifacts panel to show the live pipeline
  await page.evaluate(() => openArtifacts());
  await sleep(800);
  await shot(page, '04_pipeline_open', 'Artifacts panel open — pipeline running');

  // ── 04  Pipeline mid-run ─────────────────────────────────────────────────
  await sleep(3000);
  await shot(page, '05_pipeline_mid', 'Pipeline mid-run — AI filling fields');

  // ── 05  Wait for pipeline to complete (S.stage === 'done') ───────────────
  console.log('  waiting for pipeline to complete...');
  const done = await waitFor(page, `() => window.S && window.S.stage === 'done'`, 90000);
  if (!done) console.warn('  pipeline did not complete within timeout — capturing anyway');
  await sleep(1500);
  await shot(page, '06_pipeline_done', 'Pipeline complete — all fields filled');

  // ── 06  Switch to Fields tab in the artifacts panel ──────────────────────
  await page.evaluate(() => {
    const btn = [...document.querySelectorAll('.ap-tab')].find(t => t.textContent.trim() === 'Fields');
    if (btn) btn.click();
  });
  await sleep(800);
  await shot(page, '07_fields_extracted', 'Fields extracted with citations');

  // ── 07  Navigate to Connectors registry page ─────────────────────────────
  await page.evaluate(() => navigate('connectors'));
  await sleep(1200);
  await shot(page, '08_connectors_page', 'Connectors registry — connector registered');

  // ── 08  Open Configure for GitHub ────────────────────────────────────────
  const cfgBtn = await page.$('button:text("Configure")');
  if (cfgBtn) {
    await cfgBtn.click();
  } else {
    // fallback: click first connector tile
    await page.evaluate(() => {
      const tile = document.querySelector('.mp-conn-tile');
      if (tile) tile.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    });
  }
  await sleep(1500);
  await shot(page, '09_configure_settings', 'Configure page — settings tab');

  // ── 09  Switch to Connection (streams) tab ───────────────────────────────
  await page.evaluate(() => {
    const tab = document.getElementById('etab-connection');
    if (tab) tab.click();
  });
  await sleep(800);
  await shot(page, '10_configure_streams', 'Configure — streams & endpoints');

  // ── 10  Switch to Sync tab and run a sync ────────────────────────────────
  await page.evaluate(() => {
    const tab = document.getElementById('etab-sync');
    if (tab) tab.click();
  });
  await sleep(800);
  await shot(page, '11_sync_tab', 'Sync tab — ready to sync');

  // Click Run Sync
  const syncBtn = await page.$('#sync-run-btn, button:text("Run Sync")');
  if (syncBtn) {
    await syncBtn.click();
    await sleep(3000);
    await shot(page, '12_sync_running', 'Sync in progress — live log');

    // Wait for sync to complete
    console.log('  waiting for sync to complete...');
    await waitFor(page, `() => {
      const log = document.getElementById('sync-log');
      return log && log.textContent.includes('Completed');
    }`, 60000);
    await sleep(1500);
    await shot(page, '13_sync_done', 'Sync complete — records written');
  }

  // ── 11  Switch to Preview tab and show data table ────────────────────────
  await page.evaluate(() => {
    const tab = [...document.querySelectorAll('.sync-inner-tab')].find(t => t.textContent.trim() === 'Preview');
    if (tab) tab.click();
  });
  await sleep(1200);
  await shot(page, '14_data_preview', 'Data preview — synced records');

  // ── 12  Navigate to Observability ────────────────────────────────────────
  await page.evaluate(() => navigate('observability'));
  await sleep(1200);
  await shot(page, '15_observability', 'Observability — sync history & checkpoints');

  await browser.close();

  console.log(`\nAll screenshots saved to demo-video/public/frames/`);
  console.log('\nNext steps:');
  console.log('  cd demo-video');
  console.log('  npm install');
  console.log('  npm run render');
}

main().catch(err => {
  console.error('Capture failed:', err.message);
  process.exit(1);
});
