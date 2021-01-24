import datetime
import pytz
import re
import math
import pandas as pd

import ibk.constants


def convert_to_datetime(input_dt, tz_name=None):
    """ Convert an input date object into a datetime object.
    
        Accepts inputs of type string, datetime.datetime, datetime.date, 
          and pandas Timestamp.
        Returns a datetime.datetime object.
    """
    if isinstance(input_dt, datetime.datetime):
        return input_dt
    elif isinstance(input_dt, datetime.date):
        t = datetime.time(0)
        return datetime.datetime.combine(input_dt, t)
    elif isinstance(input_dt, str):
        return convert_datestr_to_datetime(input_dt, tz_name=tz_name)
    elif isinstance(input_dt, pd.Timestamp):
        return input_dt.to_pydatetime()
    else:
        raise ValueError('Unsupported date type: {}'.format(input_dt.__class__))
    
def convert_datestr_to_datetime(input_datestr, tz_name=None):
    """ Convert a string representing a date into a datetime.
        
        If the date has time zone information attached, then
        the result will be converted to the target time zone.
        Arguments:
            input_datestr: (str) a string representing a date or 
                datetime. Time zone information can optionally be
                included at the end of the string. If no time zone
                information is provided, then no conversion will
                be performed.
            tz_name: (str) the target time zone name to which
                the input date/time should be converted. If no
                target is provided, the target time zone will be
                assumed to be the TWS time zone.
    """
    if tz_name is None:
        tz_name = ibk.constants.TIMEZONE_TWS
    tz_tgt = pytz.timezone(tz_name)

    parts = [x for x in input_datestr.split(' ') if x]
    if re.match('[a-zA-Z]', parts[-1]) is not None:
        datestr = ' '.join(parts[:-1])

        # Get timezone objects
        tz_loc = pytz.timezone(parts[-1])

        # Get the date in the local timezone
        dt = pd.Timestamp(datestr).to_pydatetime()    
        dt_loc = tz_loc.localize(dt)

        # Convert to target time zone
        dt_tgt_full = dt_loc.astimezone(tz_tgt)
    else:
        datestr = ' '.join(parts)
        dt_tgt = tz_tgt.localize(pd.Timestamp(datestr).to_pydatetime())        

    return dt_tgt

def convert_datetime_to_tws_date(d, tz_name=None):
    if tz_name is not None:
        tzone = pytz.timezone(tz_name)
        dt = d.astimezone(tzone)
    else:
        dt = d
    return datetime.datetime.strftime(dt, '%Y%m%d %H:%M:%S')

def convert_utc_timestamp_to_datetime(tmstmp, tz_name=ibk.constants.TIMEZONE_UTC):
    tzone = pytz.timezone(tz_name)
    dt_utc = pytz.utc.localize(datetime.datetime.utcfromtimestamp(tmstmp))
    return dt_utc.astimezone(tzone)

def get_utc_datetime_from_utc_timestamp(tmstmp):
    d = datetime.datetime.utcfromtimestamp(tmstmp)
    return pytz.utc.localize(d)

def get_utc_timestamp_from_datetime(d):
    if d.tzinfo is None:
        d_tz = pytz.utc.localize(d)
    else:
        d_tz = d.astimezone(pytz.utc)
    return d_tz.timestamp()

def get_third_friday(year, month):
    """Returns the third friday, given a year and month"""
    dt = datetime.date(year, month, 1)
    if dt.weekday() <= 4:
        new_day = dt.day + 4 - dt.weekday() + 14
        third_friday = datetime.date(year, month, new_day)
    else:
        new_day = dt.day + (4 - dt.weekday()) % 7 + 14
        third_friday = datetime.date(year, month, new_day)
    return third_friday


class TimeHelper(object):
    DAYS_PER_YEAR = 365.24
    STANDARD_UNITS = ['seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years']
    UNITS_MAP = {'frequency':
                    dict(s='seconds', M='minutes', h='hours', d='days',
                         w='weeks', m='months', y='years'),
                 'duration':
                    dict(S='seconds', D='days', W='weeks', M='months', Y='years'),
                 'bar_size':
                    dict(secs='seconds', min='minutes', hour='hours', day='days',
                         week='weeks', month='months', year='years')
                }

    MAX_TWS_DURATIONS = dict(seconds={1: '1800 S', 5: '3600 S', 10: '14400 S',
                                     15: '28800 S', 30: '28800 S'},
                             minutes={1: '1 D', 2: '2 D', 3: '1 W', 5: '1 W',
                                     10: '1 W', 15: '1 W', 20: '1 W', 30: '1 M'},
                             hours={1: '1 M', 2: '1 M', 3: '1 M', 4: '1 M', 8: '1 M'},
                             days={1: '1 Y'},
                             weeks={1: '5 Y'},
                             months={1: '20 Y'},
                            )

    def __init__(self, time_val=None, time_type=None):
        super().__init__()
        if time_val is None and time_type is None:
            self.n = self.units = None
        elif 'frequency' == time_type:
            self.n, self.units = self._parse_frequency(time_val)
        elif 'duration' == time_type:
            self.n, self.units = self._parse_duration(time_val)
        elif 'bar_size' == time_type:
            self.n, self.units = self._parse_bar_size(time_val)
        else:
            raise ValueError('Unknown time type: {}'.format(time_type))

    @classmethod
    def from_attributes(cls, n, units):
        obj = cls()
        obj.n = n
        obj.units = units
        return obj

    @classmethod
    def from_timedelta(cls, delta):
        obj = cls()
        obj.n = delta.total_seconds()
        obj.units = 'seconds'
        return obj

    def to_frequency(self):
        unit = self._get_converted_type('frequency')
        return '{}{}'.format(self.n, unit)

    def to_tws_durationStr(self):
        unit = self._get_converted_type('duration')
        return '{} {}'.format(math.ceil(self.n), unit)

    def to_tws_barSizeSetting(self):
        unit = self._get_converted_type('bar_size')
        return '{} {}'.format(math.ceil(self.n), unit)

    def to_timedelta(self):
        return self._get_timedelta_from_inputs(self.n, self.units)

    def as_units(self, to_units):
        """Get a new class with different units."""
        new_n = self.n * self._get_conversion_factor(self.units, to_units)
        input_args = dict(n=new_n, units=to_units)
        return self.__class__.from_attributes(**input_args)

    def get_max_tws_duration(self):
        # Find the rule that is at least as great as the input duration
        dur_map = self.MAX_TWS_DURATIONS[self.units]
        max_dur = None
        for d in reversed(sorted(dur_map)):
            if d >= self.n:
                max_dur = dur_map[d]
        return max_dur

    def get_max_tws_duration_timedelta(self):
        max_dur = self.get_max_tws_duration()
        _n, _units = self._parse_duration(max_dur)
        return self._get_timedelta_from_inputs(_n, _units)

    def total_seconds(self):
        td = self.to_timedelta()
        return td.total_seconds()

    def get_min_tws_duration(self):
        tot_sec = self.total_seconds()
        if tot_sec < 3600 * 20:
            freq = 'seconds'
        elif tot_sec < 3600 * 24 * 13:
            freq = 'days'
        elif tot_sec < 3600 * 24 * 50:
            freq = 'weeks'
        elif tot_sec < 3600 * 24 * 450:
            freq = 'months'
        else:
            freq = 'years'

        return self.as_units(freq).to_tws_durationStr()

    def _get_timedelta_from_inputs(self, _n, _units):
        if _units in ['months', 'years']:
            # timedelta does not support months or years so we convert to days
            factor = self._get_conversion_factor(_units, 'days')
            input_args = {'days': factor * _n}
        else:
            input_args = {_units: _n}
        return datetime.timedelta(**input_args)

    def _parse_frequency(self, time_val):
        n = float(re.sub('[a-zA-Z]', '', time_val))
        orig_unit = re.sub('[\.0-9]', '', time_val)
        standard_unit = self._retrieve_unit(orig_unit, 'frequency')
        return n, standard_unit

    def _parse_duration(self, time_val):
        parsed = time_val.split(' ')
        n = math.ceil(float(parsed[0]))
        orig_unit = parsed[1]
        standard_unit = self._retrieve_unit(orig_unit, 'duration')
        return n, standard_unit

    def _parse_bar_size(self, time_val):
        parsed = time_val.split(' ')
        n = math.ceil(float(parsed[0]))
        unit = parsed[1]
        if n > 1 and (unit == 'mins' or unit == 'hours'):
            unit = unit[:-1]
        standard_unit = self._retrieve_unit(unit, 'bar_size')
        return n, standard_unit

    def _get_converted_type(self, to_type):
        unit = None
        for k, v in self.UNITS_MAP[to_type].items():
            if v == self.units:
                unit = k
        if unit is None:
            raise ValueError('Invalid conversion.')
        elif to_type == 'bar_size':
            if self.n > 1 and (unit == 'min' or unit == 'hour'):
                unit += 's'  # Make units plural for minutes and hours
        return unit

    def _retrieve_unit(self, target_unit, time_type):
        if target_unit not in self.UNITS_MAP[time_type]:
            raise ValueError('Unsupported unit for {}: {}'.format(time_type, target_unit))
        else:
            return self.UNITS_MAP[time_type][target_unit]

    def _is_valid_bar_size(self, bar_size):
        n, units = self._parse_bar_size(bar_size)
        return n in self.MAX_TWS_DURATIONS[units]

    def _get_conversion_factor(self, from_units, to_units):

        if from_units != 'seconds' and to_units != 'seconds':
            from_factor = self._get_conversion_factor(from_units, 'seconds')
            to_factor = self._get_conversion_factor(to_units, 'seconds')
            return  from_factor / to_factor
        elif from_units == 'seconds' and to_units == 'seconds':
            return 1
        elif from_units == 'seconds':
            _units = to_units
            invert = True
        elif to_units == 'seconds':
            _units = from_units
            invert = False

        if 'seconds' == _units:
            factor = 1
        elif 'minutes' == _units:
            factor = 60
        elif 'hours' == _units:
            factor = 3600
        elif 'days' == _units:
            factor = 3600 * 24
        elif 'weeks' == _units:
            factor = 3600 * 24 * 7
        elif 'months' == _units:
            factor = 3600 * 24 * self.DAYS_PER_YEAR / 12
        elif 'years' == _units:
            factor = 3600 * 24 * self.DAYS_PER_YEAR
        else:
            raise ValueError('Unknown frequency unit: {}'.format(_units))

        return factor if not invert else 1/factor

