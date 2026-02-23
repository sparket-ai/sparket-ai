/**
 * PM2 Ecosystem Configuration for Sparket Subnet - E2E Testing
 * 
 * Runs validator and 3 miners for comprehensive E2E testing.
 * Uses separate E2E database and distinct ports from test mode.
 * 
 * Control APIs:
 *   - Validator: http://127.0.0.1:8199
 *   - Miner 1 (local-miner): http://127.0.0.1:8198
 *   - Miner 2 (e2e-miner-2): http://127.0.0.1:8197
 *   - Miner 3 (e2e-miner-3): http://127.0.0.1:8196
 * 
 * Usage:
 *   pm2 start ecosystem.e2e.config.js
 *   pm2 logs e2e-validator
 *   pytest tests/e2e/localnet/ -v
 *   pm2 stop ecosystem.e2e.config.js
 */

const fs = require('fs');
const path = require('path');

const projectRoot = process.env.PROJECT_ROOT || path.resolve(__dirname);
const interpreter = path.join(projectRoot, '.venv', 'bin', 'python');
const logDir = path.join(projectRoot, 'sparket', 'logs', 'pm2-e2e');

try {
  fs.mkdirSync(logDir, { recursive: true });
} catch (error) {
  console.warn(`Unable to create PM2 log directory: ${error.message}`);
}

// Base environment for all E2E processes
const baseEnv = {
  NODE_ENV: 'e2e',
  PYTHONUNBUFFERED: '1',
  PROJECT_ROOT: projectRoot,
  SPARKET_TEST_MODE: 'true',
  TEST_MODE: 'true',
  // Localnet subtensor
  SPARKET_SUBTENSOR__NETWORK: 'local',
  SPARKET_CHAIN__ENDPOINT: 'ws://127.0.0.1:9945',
  SPARKET_CHAIN__NETUID: '2',
  // E2E database
  SPARKET_DATABASE__HOST: '127.0.0.1',
  SPARKET_DATABASE__PORT: '5435',
  SPARKET_DATABASE__NAME: 'sparket_e2e',
  SPARKET_DATABASE__USER: 'sparket',
  SPARKET_DATABASE__PASSWORD: 'sparket',
};

// Miner configurations
const miners = [
  { 
    name: 'e2e-miner-1', 
    wallet: 'local-miner', 
    axonPort: 8094, 
    controlPort: 8198,
  },
  { 
    name: 'e2e-miner-2', 
    wallet: 'e2e-miner-2', 
    axonPort: 8095, 
    controlPort: 8197,
  },
  { 
    name: 'e2e-miner-3', 
    wallet: 'e2e-miner-3', 
    axonPort: 8096, 
    controlPort: 8196,
  },
];

module.exports = {
  apps: [
    // Validator
    {
      name: 'e2e-validator',
      script: path.join(projectRoot, 'sparket/entrypoints/validator.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      args: '--logging.trace --logging.debug --logging.info',
      
      env: {
        ...baseEnv,
        SPARKET_ROLE: 'validator',
        SPARKET_WALLET__NAME: 'local-validator',
        SPARKET_WALLET__HOTKEY: 'default',
        SPARKET_AXON__HOST: '0.0.0.0',
        SPARKET_AXON__PORT: '8093',
        // Ledger endpoint for auditor validators
        SPARKET_LEDGER__ENABLED: 'true',
        SPARKET_LEDGER__HTTP_PORT: '8200',
        SPARKET_LEDGER__MIN_STAKE_THRESHOLD: '0',
        SPARKET_LEDGER__DATA_DIR: path.join(projectRoot, 'sparket/data/ledger'),
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '2G',
      min_uptime: '10s',
      max_restarts: 5,
      restart_delay: 5000,
      
      error_file: path.join(logDir, 'e2e-validator-error.log'),
      out_file: path.join(logDir, 'e2e-validator-out.log'),
      log_file: path.join(logDir, 'e2e-validator-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
    },
    
    // SDIO data ingestor
    {
      name: 'e2e-ingestor',
      script: path.join(projectRoot, 'sparket/entrypoints/ingestor.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      env: {
        ...baseEnv,
        SPARKET_ROLE: 'ingestor',
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      min_uptime: '10s',
      max_restarts: 5,
      restart_delay: 5000,
      
      error_file: path.join(logDir, 'e2e-ingestor-error.log'),
      out_file: path.join(logDir, 'e2e-ingestor-out.log'),
      log_file: path.join(logDir, 'e2e-ingestor-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
    },
    
    // Miners
    ...miners.map(m => ({
      name: m.name,
      script: path.join(projectRoot, 'sparket/entrypoints/miner.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      args: `--wallet.name ${m.wallet} --wallet.hotkey default --axon.port ${m.axonPort}`,
      
      env: {
        ...baseEnv,
        SPARKET_ROLE: 'miner',
        SPARKET_WALLET__NAME: m.wallet,
        SPARKET_WALLET__HOTKEY: 'default',
        SPARKET_AXON__HOST: '0.0.0.0',
        SPARKET_AXON__PORT: String(m.axonPort),
        SPARKET_MINER_API_PORT: String(m.controlPort),
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      min_uptime: '10s',
      max_restarts: 5,
      restart_delay: 5000,
      
      error_file: path.join(logDir, `${m.name}-error.log`),
      out_file: path.join(logDir, `${m.name}-out.log`),
      log_file: path.join(logDir, `${m.name}-combined.log`),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
    })),
    
    // Auditor validator (uses e2e-miner-3 wallet, repurposed)
    {
      name: 'e2e-auditor',
      script: path.join(projectRoot, 'sparket/entrypoints/auditor.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      args: '--wallet.name e2e-miner-3 --wallet.hotkey default --subtensor.chain_endpoint ws://127.0.0.1:9945 --netuid 2 --logging.trace --logging.debug --logging.info',
      
      env: {
        ...baseEnv,
        SPARKET_ROLE: 'auditor',
        SPARKET_WALLET__NAME: 'e2e-miner-3',
        SPARKET_WALLET__HOTKEY: 'default',
        SPARKET_AUDITOR__PRIMARY_HOTKEY: '5HKjkxQxGrVZDHZKVHRQua1nTnvHgEz8mX67wqVzgNU6a3EK',
        SPARKET_AUDITOR__PRIMARY_URL: 'http://127.0.0.1:8200',
        SPARKET_AUDITOR__POLL_INTERVAL_SECONDS: '30',
        SPARKET_AUDITOR__WEIGHT_TOLERANCE: '0.01',
        SPARKET_AUDITOR__DATA_DIR: path.join(projectRoot, 'sparket/data/auditor'),
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      min_uptime: '10s',
      max_restarts: 5,
      restart_delay: 5000,
      
      error_file: path.join(logDir, 'e2e-auditor-error.log'),
      out_file: path.join(logDir, 'e2e-auditor-out.log'),
      log_file: path.join(logDir, 'e2e-auditor-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
    },
  ],
};
