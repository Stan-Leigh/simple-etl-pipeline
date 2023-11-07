from etl_process import clean_data, append_time_column
import pandas as pd
import mongomock

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
    connection = mongomock.Connection()

    db = connection.db
    collection = db.collection

    data_to_insert = df.to_dict('records')

    # Inserting values to table test 
    x = collection.insert(data_to_insert) 

    assert len(x) == 100
