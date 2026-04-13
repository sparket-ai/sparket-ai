/**
 * PM2 Ecosystem Configuration for Sparket Subnet
 * 
 * Usage:
 *   pm2 start ecosystem.config.js          # Start all processes
 *   pm2 start ecosystem.config.js --only validator-local  # Start specific process
 *   pm2 stop ecosystem.config.js           # Stop all processes
 *   pm2 restart ecosystem.config.js         # Restart all processes
 *   pm2 delete ecosystem.config.js          # Delete all processes
 *   pm2 logs validator-local               # View logs
 *   pm2 monit                              # Monitor processes
 * 
 * Note: This config automatically loads environment variables from .env file
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
        // Skip comments and empty lines
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) {
          continue;
        }
        
        // Parse KEY=VALUE format
        const match = trimmed.match(/^([^=]+)=(.*)$/);
        if (match) {
          const key = match[1].trim();
          let value = match[2].trim();
          
          // Remove quotes if present
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
const scriptPath = path.join(projectRoot, 'sparket/entrypoints/validator.py');
const logDir =
  envVars.PM2_LOG_DIR ||
  process.env.PM2_LOG_DIR ||
  path.join(projectRoot, 'sparket', 'logs', 'pm2');

try {
  fs.mkdirSync(logDir, { recursive: true });
} catch (error) {
  console.warn(`Unable to create PM2 log directory at ${logDir}: ${error.message}`);
}

// Which process group to start.  Default is "primary" (validator + ingestor).
// Set SPARKET_PM2_PROFILE=auditor to start the auditor instead.
const profile = (envVars.SPARKET_PM2_PROFILE || process.env.SPARKET_PM2_PROFILE || 'primary').toLowerCase();

console.log(
  `PM2 will run from ${projectRoot} [profile=${profile}]. Loaded ${Object.keys(envVars).length} environment variables from .env`
);

const validatorApp = {
      name: 'validator-local',
      script: scriptPath,
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      // Environment variables - merge .env file vars with defaults
      // .env file variables take precedence
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        SPARKET_ROLE: 'validator',
        PROJECT_ROOT: projectRoot,
        PM2_LOG_DIR: logDir,
        VENV_PYTHON: interpreter,
        SPARKET_AXON__HOST: envVars.SPARKET_AXON__HOST || '0.0.0.0',
        SPARKET_AXON__PORT: envVars.SPARKET_AXON__PORT || '8093',
        // Merge all .env variables
        ...envVars,
      },
      
      // Auto-restart configuration
      autorestart: true,
      watch: false,
      max_memory_restart: '8G',
      
      // Restart behavior
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      // Logging
      error_file: path.join(logDir, 'validator-local-error.log'),
      out_file: path.join(logDir, 'validator-local-out.log'),
      log_file: path.join(logDir, 'validator-local-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      // Process management
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
      
      // Advanced options
      instance_var: 'NODE_APP_INSTANCE',
      pmx: true,
      automation: true,
      vizion: true,
};

const ingestorApp = {
      name: 'ingestor-local',
      script: path.join(projectRoot, 'sparket/entrypoints/ingestor.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        SPARKET_ROLE: 'ingestor',
        PROJECT_ROOT: projectRoot,
        PM2_LOG_DIR: logDir,
        VENV_PYTHON: interpreter,
        ...envVars,
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '3G',
      
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      error_file: path.join(logDir, 'ingestor-local-error.log'),
      out_file: path.join(logDir, 'ingestor-local-out.log'),
      log_file: path.join(logDir, 'ingestor-local-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
};

const auditorApp = {
      name: 'auditor-local',
      script: path.join(projectRoot, 'sparket/entrypoints/auditor.py'),
      interpreter,
      cwd: projectRoot,
      instances: 1,
      exec_mode: 'fork',
      
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        SPARKET_ROLE: 'auditor',
        PROJECT_ROOT: projectRoot,
        PM2_LOG_DIR: logDir,
        VENV_PYTHON: interpreter,
        ...envVars,
      },
      
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      
      min_uptime: '10s',
      max_restarts: 10,
      restart_delay: 4000,
      
      error_file: path.join(logDir, 'auditor-local-error.log'),
      out_file: path.join(logDir, 'auditor-local-out.log'),
      log_file: path.join(logDir, 'auditor-local-combined.log'),
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      kill_timeout: 5000,
      wait_ready: false,
      listen_timeout: 10000,
};

// Build apps list based on profile
const profiles = {
  primary: [validatorApp, ingestorApp],
  auditor: [auditorApp],
  all: [validatorApp, ingestorApp, auditorApp],
};

module.exports = {
  apps: profiles[profile] || profiles.primary,
};

