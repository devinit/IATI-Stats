from lxml import etree
import datetime

from stats.dashboard import ActivityStats


def test_comperhensiveness_is_current():
    activity_stats = ActivityStats()
    activity_stats.today = datetime.date(9990, 6, 1)

    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <activity-status code="2"/>
        </iati-activity>
    ''')
    assert activity_stats._comprehensiveness_is_current()

    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <activity-status code="3"/>
        </iati-activity>
    ''')
    assert not activity_stats._comprehensiveness_is_current()

    activity_stats.element = etree.fromstring('''
        <iati-activity>
        </iati-activity>
    ''')
    assert activity_stats._comprehensiveness_is_current()

    def end_planned_date(datestring):
        return etree.fromstring('''
            <iati-activity>
                <activity-date type="end-planned" iso-date="{}"/>
            </iati-activity>
        '''.format(datestring))
    
    # Any end dates in a year before this year should be current
    activity_stats.element = end_planned_date('9989-06-01')
    assert not activity_stats._comprehensiveness_is_current()
    activity_stats.element = end_planned_date('9989-12-31')
    assert not activity_stats._comprehensiveness_is_current()

    # Any end dates in a year after this year should be current
    activity_stats.element = end_planned_date('9990-01-01')
    assert activity_stats._comprehensiveness_is_current()
    activity_stats.element = end_planned_date('9990-01-01')
    assert activity_stats._comprehensiveness_is_current()
    activity_stats.element = end_planned_date('9990-06-01')
    assert activity_stats._comprehensiveness_is_current()
    activity_stats.element = end_planned_date('9990-06-02')
    assert activity_stats._comprehensiveness_is_current()
    activity_stats.element = end_planned_date('9991-06-01')
    assert activity_stats._comprehensiveness_is_current()

    def datetype(typestring):
        return etree.fromstring('''
            <iati-activity>
                <activity-date type="{}" iso-date="9989-06-01"/>
            </iati-activity>
        '''.format(typestring))

    # Ignore start dates
    activity_stats.element = datetype('start-actual')
    assert activity_stats._comprehensiveness_is_current()
    activity_stats.element = datetype('start-planned')
    assert activity_stats._comprehensiveness_is_current()

    # But use all end dates
    activity_stats.element = datetype('end-actual')
    assert not activity_stats._comprehensiveness_is_current()
    activity_stats.element = datetype('end-planned')
    assert not activity_stats._comprehensiveness_is_current()

    # If there are two end dates, and one of them is in the future, then it is current
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <activity-date type="end-planned" iso-date="9989-06-01"/>
            <activity-date type="end-actual" iso-date="9990-12-31"/>
y
        </iati-activity>
    ''')
    assert activity_stats._comprehensiveness_is_current()

    # Activity status should take priority over activity date
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <activity-status code="2"/> 
            <activity-date type="end-actual" iso-date="9990-12-31"/>
y
        </iati-activity>
    ''')
    assert activity_stats._comprehensiveness_is_current()

    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <activity-status code="4"/> 
            <activity-date type="end-actual" iso-date="9990-06-01"/>
y
        </iati-activity>
    ''')
    assert not activity_stats._comprehensiveness_is_current()

def test_comperhensiveness_denominator():
    pass

def test_comperhensiveness_empty():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
        <iati-activity>
        </iati-activity>
    ''')
    assert activity_stats.comprehensiveness() == {
        'reporting-org': 0,
        'iati-identifier': 0,
        'participating-org': 0,
        'title': 0,
        'description': 0,
        'activity-status': 0,
        'activity-date': 0,
        'sector': 0,
        'country_or_region': 0
    }

def test_comprehensiveness_full():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <reporting-org/>
            <iati-identifier/>
            <participating-org/>
            <title/>
            <description/>
            <activity-status/>
            <activity-date/>
            <sector/>
            <recipient-country/>
        </iati-activity>
    ''')
    assert activity_stats.comprehensiveness() == {
        'reporting-org': 1,
        'iati-identifier': 1,
        'participating-org': 1,
        'title': 1,
        'description': 1,
        'activity-status': 1,
        'activity-date': 1,
        'sector': 1,
        'country_or_region': 1
    }

    # Check recipient-region independently
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <transaction>
                <recipient-region/>
            </transaction>
        </iati-activity>
    ''')
    comprehensiveness = activity_stats.comprehensiveness()
    assert comprehensiveness['country_or_region'] == 1


def comprehensiveness_transaction_level_elements():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <transaction>
                <sector/>
                <recipient-country/>
            </transaction>
        </iati-activity>
    ''')
    comprehensiveness = activity_stats.comprehensiveness()
    assert comprehensiveness['sector'] == 1
    assert comprehensiveness['country_or_region'] == 1

    # Check recipient-region too
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <recipient-region/>
        </iati-activity>
    ''')
    comprehensiveness = activity_stats.comprehensiveness()
    assert comprehensiveness['country_or_region'] == 1

    # If is only at transaction level, but not for all transactions, we should get 0
    activity_stats.element = etree.fromstring('''
        <iati-activity>
            <transaction>
                <sector/>
                <recipient-country/>
                <recipient-region/>
            </transaction>
            <transaction></transaction>
        </iati-activity>
    ''')
    comprehensiveness = activity_stats.comprehensiveness()
    assert comprehensiveness['sector'] == 0
    assert comprehensiveness['country_or_region'] == 0