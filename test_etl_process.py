from etl_process import etl_pipeline

def test_etl_process():
    assert etl_pipeline() == "Success"