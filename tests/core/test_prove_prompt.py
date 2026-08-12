from pathlib import Path

from core.tasks import tickets as T


class TestClickIndicatorInjection:
    def test_asset_loads_and_has_single_listener(self):
        script = T._click_indicator_script()
        assert "frshty-click-indicator-style" in script
        assert script.count("addEventListener") == 1

    def test_prompt_includes_script_and_suppresses_documentation(self):
        script = T._click_indicator_script()
        prompt = T._PROVE_PROMPT_TEMPLATE.format(proof_md="GUIDE")
        prompt += T._PROVE_BROWSER_BLOCK_HEADER + script + T._PROVE_BROWSER_BLOCK_FOOTER
        assert "GUIDE" in prompt
        assert "addInitScript" in prompt
        assert "Do NOT mention it in docs/proof.md" in prompt
        assert "translate(-50%, -50%)" in prompt

    def test_missing_asset_degrades_to_empty(self, monkeypatch, tmp_path):
        monkeypatch.setattr(T, "__file__", str(tmp_path / "a" / "b" / "c.py"))
        assert T._click_indicator_script() == ""
