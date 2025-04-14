if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Host": "enabler.orbiz.id",
            "Priority": "u=0, i",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
            "Cookie":"_ga_1RR23X09YM=GS1.1.1740214595.9.1.1740214619.0.0.0; _ga=GA1.1.1585873770.1726721190; XSRF-TOKEN=eyJpdiI6IkF1UG5jYzRTZ3I0TGpySXpLNFdFQXc9PSIsInZhbHVlIjoieEVFK3dNdTl5S2w5TVlBWlhEb2xGSnk4MkhaT21wNWlldkhjaDJtczRtVm1QZ2ZVQ2UzSzZXSXZnY0kxdWpJNURQV25qTzh5VEhzUU0xQ0ZDcG9FSmhodmZEaGNaNzNJRlVhc3RrUi8zMTMvRWFyRnpaSVFWcE10aVEycXJqbDYiLCJtYWMiOiI2MDczZTllYjc1NmU4MTg5YzRhMGRhYjY5OTViOGE4MjJlNmQ0NGRkOTNlMDI1MzgwZGQ5NjE3MGQ5MWE3YmJkIiwidGFnIjoiIn0%3D; orbiz_e_commerce_enabler_session=eyJpdiI6Ijc5YW1mWTE5OEl1MXhBUXd6dDIvOUE9PSIsInZhbHVlIjoiT0pacEE5a09oZ1ViSnk0bVJHMWhjWG1WcEwreVFzR2QwckxPdkN1aklURGh4ZmQyb3BGOXUvR3NWRmw4OVRXZlg2MWhmbEFNbHg2Y0g2OENpcEpxWFdlUzlnVXlmSVA2RUUyOVB0Q3hVakE4R0VjTmtxaFhkYXFGQ2NBck1iSUUiLCJtYWMiOiIxYzlmZWQ1NzdjMTYzNmRmZDk5ZjU5MDk1YTc2MjliOTM1Y2RkNTU2MzdjYTcyYzFhYTMxZWFkYTEzY2RlNmVkIiwidGFnIjoiIn0%3D"
    }
    import requests
    import pprint
    import json
    response=requests.get('https://enabler.orbiz.id/noverse/26',headers=headers)
    data_token=json.loads(response.text)
    # Specify your data loading logic here

    return data_token


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
