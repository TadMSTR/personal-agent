module.exports = {
  apps: [
    {
      name: "personal-agent",
      script: "/home/ted/repos/personal/personal-agent/start.sh",
      interpreter: "bash",
      cwd: "/home/ted/repos/personal/personal-agent",
      restart_delay: 5000,
      max_restarts: 10,
      out_file: "/home/ted/.pm2/logs/personal-agent-out.log",
      error_file: "/home/ted/.pm2/logs/personal-agent-error.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
