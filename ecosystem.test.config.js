/**
 * PM2 Ecosystem Configuration for Sparket Subnet - TEST MODE
 * 
 * This config runs both validator and miner nodes in test mode for local development.
 * Uses separate databases and docker containers from production.
 * 
 * Test Mode Control APIs:
 *   - Validator: http://127.0.0.1:8199 (seed data, trigger scoring)
 *   - Miner: http://127.0.0.1:8198 (fetch games, submit odds)
 * 
 * Usage:
 *   pm2 start ecosystem.test.config.js          # Start all test processes
 *   pm2 start ecosystem.test.config.js --only test-validator  # Start specific process
 *   pm2 stop ecosystem.test.config.js           # Stop all test processes
 *   pm2 delete ecosystem.test.config.js         # Delete all test processes
 *   pm2 logs test-validator                     # View validator logs
 *   pm2 logs test-miner                         # View miner logs
 *   pm2 monit                                   # Monitor processes
 * 
 * E2E Testing:
 *   python scripts/test_e2e.py                  # Run E2E test suite
 */

const fs = require('fs');
const path = require('path');

// Load .env file and parse environment variables
function loadEnvFile(envPath) {
  const env = {};
  try {
    if (fs.existsSync(envPath)) {
      const content = fs.readFileSync(envPath, 'utf8');
      const lines = content.split('\n');
      
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        
        const match = trimmed.match(/^([^=]+)=(.*)$/);
        if (match) {
          const key = match[1].trim();
          let value = match[2].trim();
          
          if ((value.startsWith('"') && value.endsWith('"')) ||
              (value.startsWith("'") && value.endsWith("'"))) {
            value = value.slice(1, -1);
          }
          
          env[key] = value;
        }
      }
    }
  } catch (error) {
    console.error(`Error loading .env file: ${error.message}`);
  }
  return env;
}

// Load .env file from project root
const defaultProjectRoot = process.env.PROJECT_ROOT || path.resolve(__dirname);
const envPath = path.join(defaultProjectRoot, '.env');
const envVars = loadEnvFile(envPath);

const projectRoot = envVars.PROJECT_ROOT || defaultProjectRoot;
const interpreter =
  envVars.VENV_PYTHON ||
  process.env.VENV_PYTHON ||
  path.join(projectRoot, '.venv', 'bin', 'python');

const validatorScript = path.join(projectRoot, 'sparket/entrypoints/validator.py');
const minerScript = path.join(projectRoot, 'sparket/entrypoints/miner.py');

const logDir =
  envVars.PM2_LOG_DIR ||
  process.env.PM2_LOG_DIR ||
  path.join(projectRoot, 'sparket', 'logs', 'pm2-test');

try {
  fs.mkdirSync(logDir, { recursive: true });
} catch (error) {
  console.warn(`Unable to create PM2 log directory at ${logDir}: ${error.message}`);
}

console.log(`PM2 TEST MODE - running from ${projectRoot}`);
console.log(`Test data directory: ${path.join(projectRoot, 'sparket', 'data', 'test')}`);

// Test mode environment overrides
const testEnvBase = {
  NODE_ENV: 'test',
  PYTHONUNBUFFERED: '1',
  PROJECT_ROOT: projectRoot,
  PM2_LOG_DIR: logDir,
  VENV_PYTHON: interpreter,
  // Enable test mode - uses separate data directories and docker containers
  SPARKET_TEST_MODE: 'true',
  TEST_MODE: 'true',
  // Local network settings
  SPARKET_SUBTENSOR__NETWORK: 'local',
  SPARKET_CHAIN__ENDPOINT: 'ws://127.0.0.1:9945',
  // Test mode uses same postgres container, different database
  SPARKET_DATABASE__PORT: '5435',
  SPARKET_DATABASE__NAME: 'sparket_test',
};

module.exports = {
  apps: [
    {
      name: 'test-validator',
      script: validatorScript,
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      // Enable logging via CLI args (boolean flags, no values needed)
      // Use real localnet chain (validator registered on netuid 2)
      args: '--logging.trace --logging.debug --logging.info',
      
      env: {
        ...testEnvBase,
        SPARKET_ROLE: 'validator',
        SPARKET_AXON__HOST: '0.0.0.0',
        SPARKET_AXON__PORT: '8093',
        // Database credentials (use test defaults or env)
        SPARKET_DATABASE__USER: envVars.SPARKET_DATABASE__USER || 'sparket',
        SPARKET_DATABASE__PASSWORD: envVars.SPARKET_DATABASE__PASSWORD || 'sparket',
        SPARKET_DATABASE__HOST: envVars.SPARKET_DATABASE__HOST || '127.0.0.1',
        // Note: NOT spreading envVars here to avoid overriding test settings
        // Force test mode overrides
        SPARKET_TEST_MODE: 'true',
        SPARKET_DATABASE__PORT: '5435',
        SPARKET_DATABASE__NAME: 'sparket_test',
        // Force wallet for validator (UID 1)
        SPARKET_WALLET__NAME: 'test-localnet',
        SPARKET_WALLET__HOTKEY: 'default',
        // Observability sync to host server (pass through from .env)
        SPARKET_OBSERVABILITY__SYNC_ENABLED: envVars.SPARKET_OBSERVABILITY__SYNC_ENABLED || 'false',
        SPARKET_OBSERVABILITY__SYNC_API_URL: envVars.SPARKET_OBSERVABILITY__SYNC_API_URL || '',
        SPARKET_OBSERVABILITY__SYNC_API_KEY: envVars.SPARKET_OBSERVABILITY__SYNC_API_KEY || '',
        SPARKET_OBSERVABILITY__SYNC_TIMEOUT_SECONDS: envVars.SPARKET_OBSERVABILITY__SYNC_TIMEOUT_SECONDS || '10',
        SPARKET_OBSERVABILITY__HEARTBEAT_INTERVAL_STEPS: envVars.SPARKET_OBSERVABILITY__HEARTBEAT_INTERVAL_STEPS || '5',
        // SportsDataIO key for ingestor data
        SDIO_API_KEY: envVars.SDIO_API_KEY || '',
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '2G',
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      error_file: path.join(logDir, 'test-validator-error.log'),
      out_file: path.join(logDir, 'test-validator-out.log'),
      log_file: path.join(logDir, 'test-validator-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
    },
    
    // SDIO data ingestor
    {
      name: 'test-ingestor',
      script: path.join(projectRoot, 'sparket/entrypoints/ingestor.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      env: {
        ...testEnvBase,
        SPARKET_ROLE: 'ingestor',
        SPARKET_DATABASE__USER: envVars.SPARKET_DATABASE__USER || 'sparket',
        SPARKET_DATABASE__PASSWORD: envVars.SPARKET_DATABASE__PASSWORD || 'sparket',
        SPARKET_DATABASE__HOST: envVars.SPARKET_DATABASE__HOST || '127.0.0.1',
        SPARKET_TEST_MODE: 'true',
        SPARKET_DATABASE__PORT: '5435',
        SPARKET_DATABASE__NAME: 'sparket_test',
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      error_file: path.join(logDir, 'test-ingestor-error.log'),
      out_file: path.join(logDir, 'test-ingestor-out.log'),
      log_file: path.join(logDir, 'test-ingestor-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
    },
    
    {
      name: 'test-miner',
      script: minerScript,
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      // Use CLI args that bittensor will actually parse
      args: '--wallet.name local-miner --wallet.hotkey default --axon.port 8094',
      
      env: {
        ...testEnvBase,
        SPARKET_ROLE: 'miner',
        SPARKET_AXON__HOST: '0.0.0.0',
        SPARKET_AXON__PORT: '8094',
        ...envVars,
        // Force test mode overrides AFTER env merge (these take precedence)
        SPARKET_TEST_MODE: 'true',
        // Force wallet for miner (UID 2)
        SPARKET_WALLET__NAME: 'local-miner',
        SPARKET_WALLET__HOTKEY: 'default',
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      error_file: path.join(logDir, 'test-miner-error.log'),
      out_file: path.join(logDir, 'test-miner-out.log'),
      log_file: path.join(logDir, 'test-miner-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
    },
  ],
};

