"""
This is a stats module, you can use it by running (in the parent directory)
python calculate_stats.py --stats-module stats.timeliness_requirements loop

It calculates just the stats required for the Dashboard timeliness calculation in `IATI/IATI-Dashboard/timeliness.py`
"""
import stats.dashboard

class PublisherStats(stats.dashboard.PublisherStats):
    enabled_stats = ['timelag']

class ActivityFileStats(object):
    pass

class ActivityStats(stats.dashboard.ActivityStats):
    enabled_stats = ['most_recent_transaction_date', 'transaction_months_with_year']

class OrganisationFileStats(object):
    pass

class OrganisationStats(object):
    pass

class AllDataStats(object):
    pass

