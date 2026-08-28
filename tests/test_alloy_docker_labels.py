from pathlib import Path


def test_alloy_uses_stable_compose_identity_and_host() -> None:
    template = Path(
        "../puppet-control-repo/modules/profile/templates/alloy.config.epp"
    ).read_text()

    assert "__meta_docker_container_label_com_docker_compose_project" in template
    assert "__meta_docker_container_label_com_docker_compose_service" in template
    assert '["service_name", "__meta_docker_container_name"]' in template
    assert 'target_label = "host"' in template
    assert "<%= $trusted['certname'] %>" in template
