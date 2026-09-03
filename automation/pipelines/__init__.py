from automation_core.connections import VaultConnections


def run_pipeline(name: str, vault: VaultConnections) -> None:
    if name == "exam":
        from automation.pipelines.exam import run
    elif name == "download-zanzara":
        from automation.pipelines.download_zanzara import run
    elif name == "puppet-release-watch":
        from automation.pipelines.puppet_release_watch import run
    elif name == "cyber-analyst":
        from automation.pipelines.cyber_analyst import run
    elif name == "operations-analyst":
        from automation.pipelines.operations_analyst import run
    elif name == "podcast-statistics":
        from automation.pipelines.podcast_statistics import run
    else:
        raise ValueError(f"Unknown pipeline: {name}")
    run(vault)
