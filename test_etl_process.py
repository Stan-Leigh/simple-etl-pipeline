from etl_process import clean_data, append_time_column, etl_pipeline
import pandas as pd

def test_clean_data():
    df = pd.read_csv('mock_data.csv')
    result = clean_data(df)

    assert result.isnull().sum().sum() == 0


def test_append_time_column():
    df = pd.read_csv('mock_data.csv')
    result = append_time_column(df)

    response = "inserted_at" in result.columns

    assert response == True


def test_etl_pipeline(mocker):
    df = pd.read_csv('mock_data.csv')
    mocker.patch("etl_process.etl_pipeline", 100)

    expected = 100

    actual = etl_pipeline(df)

    assert expected == actual
