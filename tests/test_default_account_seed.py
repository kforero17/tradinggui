from src.config.settings import settings
from src.data.database import Database


def test_renamed_default_account_is_not_reseeded(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "seed.db")
    db = Database()
    db.rename_account(settings.DEFAULT_ACCOUNT, "taxable")

    reopened = Database()

    assert [a["name"] for a in reopened.get_accounts()] == ["taxable"]


def test_empty_accounts_table_gets_default_account(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "seed.db")

    db = Database()

    assert [a["name"] for a in db.get_accounts()] == [settings.DEFAULT_ACCOUNT]
