import pandas as pd
from src.etl.bea_client import BEAClient


def test_env_key_loaded(monkeypatch):
    monkeypatch.setenv("BEA_API_KEY", "TEST_KEY")
    client = BEAClient()
    assert client.api_key == "TEST_KEY"


def test_bea_request_mocked(mocker):
    # Mock odpowiedzi BEA API
    fake_response = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {"Year": "2020", "Industry": "Manufacturing", "DataValue": "123"},
                    {"Year": "2021", "Industry": "Finance", "DataValue": "456"}
                ]
            }
        }
    }

    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = fake_response

    client = BEAClient()
    df = client.get_data("GDPbyIndustry", "VA")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "DataValue" in df.columns
    assert df["DataValue"].dtype in [float, int]


def test_save_to_csv(tmp_path, mocker):
    # Mock API
    fake_response = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {"Year": "2020", "Industry": "Manufacturing", "DataValue": "789"}
                ]
            }
        }
    }

    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = fake_response

    # Ustawienie tmp folderu
    tmp_file = tmp_path / "output.csv"

    client = BEAClient()
    client.save_to_csv("GDPbyIndustry", "VA", str(tmp_file))

    assert tmp_file.exists()

    df = pd.read_csv(tmp_file)
    assert len(df) == 1
    assert df.iloc[0]["DataValue"] == 789.0
