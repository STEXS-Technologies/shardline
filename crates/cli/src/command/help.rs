pub const CLI_AFTER_LONG_HELP: &str = "\
Examples:
  shardline providerless setup
  shardline serve --role all
  shardline config check
  shardline gc --mark --sweep --retention-seconds 86400
  shardline gc schedule install --env-file /etc/shardline/shardline.env --user shardline --group shardline
  shardline storage migrate --from local --from-root /srv/assets/.shardline/data --to s3 --prefix xorbs/default/
  shardline bench --mode ingest --iterations 5 --concurrency 16
  shardline completion bash > /usr/share/bash-completion/completions/shardline
  shardline manpage --output ./shardline.1";

pub const GC_INSTALL_AFTER_LONG_HELP: &str = "\
Examples:
  shardline gc schedule install --output-dir ./systemd --env-file /etc/shardline/shardline.env --user shardline --group shardline
  shardline gc schedule install --calendar 'hourly' --retention-seconds 600 --binary-path /usr/local/bin/shardline";

pub const BENCH_AFTER_LONG_HELP: &str = "\
Examples:
  shardline bench --storage-dir /var/lib/shardline-bench
  shardline bench --storage-dir /var/lib/shardline-bench --deployment-target configured
  shardline bench --storage-dir /var/lib/shardline-bench --scenario cross-repository-upload --iterations 5
  shardline bench --mode ingest --iterations 10 --concurrency 32 --chunk-size-bytes 1048576";

pub const COMPLETION_AFTER_HELP: &str = "\
Examples:
  shardline completion bash
  shardline completion zsh --output ./_shardline
  shardline completion fish --output ~/.config/fish/completions/shardline.fish";

pub const MANPAGE_AFTER_HELP: &str = "\
Examples:
  shardline manpage
  shardline manpage --output ./shardline.1";
