from mage_ai.orchestration.run_status_checker import check_status


@sensor
def check_condition(*args, **kwargs) -> bool:
    return check_status(
        'etl_enabler_dashboard',
        kwargs['execution_date'],
        hours=24,
    )