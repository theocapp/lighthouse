from lighthouse.config import load_config


def test_data_year_defaults_load_correctly(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text("{}", encoding="utf-8")

    cfg = load_config(str(config_path))

    assert cfg.data.disclosure_year == 2024
    assert cfg.data.ptr_year == 2024
    assert cfg.data.fec_cycle == 2024
