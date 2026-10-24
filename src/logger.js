const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

const C = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
};

const useColor = process.stdout.isTTY || process.env.FORCE_COLOR === '1';
const paint = (color, s) => (useColor ? `${C[color]}${s}${C.reset}` : s);
const LEVEL_COLOR = { debug: 'dim', info: 'cyan', warn: 'yellow', error: 'red' };

export function createLogger(name, level = process.env.LOG_LEVEL || 'info') {
  const min = LEVELS[level] ?? LEVELS.info;
  const log = (lvl, args) => {
    if (LEVELS[lvl] < min) return;
    const ts = new Date().toISOString().slice(11, 23);
    const tag = paint(LEVEL_COLOR[lvl], lvl.toUpperCase().padEnd(5));
    const out = lvl === 'error' || lvl === 'warn' ? console.error : console.log;
    out(`${paint('dim', ts)} ${tag} [${name}]`, ...args);
  };
  return {
    debug: (...a) => log('debug', a),
    info: (...a) => log('info', a),
    warn: (...a) => log('warn', a),
    error: (...a) => log('error', a),
  };
}

export const silentLogger = { debug() {}, info() {}, warn() {}, error() {} };
export { paint };
