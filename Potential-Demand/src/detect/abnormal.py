"""
This module provides a class for finding the hours with
no cars available to borrow or return

"""

import datetime


class Base:
    """
    Find Date, Hour, and Stop ID Based on Given Criteria

    """
    def __init__(self):
        pass

    def find_date_hour_stop_id(
        self, df, issue, tolerate_num, tolerate_time,
        date_m6h, stop_id, result
    ):

        if issue == 'empty':
            time_index = list(
                df[df.available_rent_bikes <= tolerate_num].index
            )
        elif issue == 'full':
            df['available_return_bikes'] = df['capacity'] - df['available_rent_bikes']
            time_index = list(
                df[df.available_return_bikes <= tolerate_num].index
            )

        non_time_index = list(
            set(df.index).difference(set(time_index))
        )
        time_interval = []
        time_index.sort()
        non_time_index.sort()
        hour = [
            datetime.datetime.strptime(f'{date_m6h} 0{x}:00:00', '%Y-%m-%d %H:%M:%S')
            if x<10
            else datetime.datetime.strptime(f'{date_m6h} {x}:00:00', '%Y-%m-%d %H:%M:%S')
            for x in range(24)
        ]
        hour.append(
            datetime.datetime.strptime(f'{date_m6h} 23:59:59', '%Y-%m-%d %H:%M:%S')
        )

        for x in time_index:
            try:
                next_non_time_index = [t for t in non_time_index if t > x][0]
            except: # pylint: disable=bare-except
                result.append(
                    (date_m6h, int(df.loc[x, 'time'][:2]), stop_id)
                )

            try:
                time_interval.append(
                    (df.loc[x, 'adjust_api_time'], df.loc[next_non_time_index, 'adjust_api_time'])
                )
            except: # pylint: disable=bare-except
                pass

        non_cross_time_interval = [
            x for x in time_interval
            if x[0][11:13] == x[1][11:13]
        ]
        cross_time_interval = [
            x for x in time_interval
            if x[0][11:13] != x[1][11:13]
        ]
        cross_cross_time_interval = [
            x for x in time_interval
            if (int(x[1][11:13]) - int(x[0][11:13])) > 1
        ]

        # the period of time interval exceed threshold
        for x in non_cross_time_interval:
            delta = datetime.datetime.strptime(
                x[1], '%Y-%m-%d %H:%M:%S'
            ) - datetime.datetime.strptime(
                x[0], '%Y-%m-%d %H:%M:%S'
            )
            if delta.seconds > (tolerate_time* 60):
                result.append(
                    (date_m6h, int(x[0][11:13]), stop_id)
                )

        for x in cross_time_interval:
            # start time
            tem_start_time = datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S')
            tem = datetime.datetime.strptime(
                f'{date_m6h} 23:59:59', '%Y-%m-%d %H:%M:%S'
            )
            if tem_start_time < tem:
                comp_start_time = [t for t in hour if t > tem_start_time][0]
                delta = comp_start_time - tem_start_time
                if delta.seconds > (tolerate_time* 60):
                    result.append(
                        (date_m6h, int(x[0][11:13]), stop_id)
                    )

            # end time
            tem_end_time = datetime.datetime.strptime(
                x[1], '%Y-%m-%d %H:%M:%S'
            )
            tem = datetime.datetime.strptime(
                f'{date_m6h} 23:59:59', '%Y-%m-%d %H:%M:%S'
            )
            if tem_end_time < tem:
                comp_end_time = [t for t in hour if t < tem_end_time][-1]
                delta = tem_end_time - comp_end_time
                if delta.seconds > (tolerate_time* 60):
                    result.append(
                        (date_m6h, int(x[1][11:13]), stop_id)
                    )

        for x in cross_cross_time_interval:
            s = int(x[0][11:13])
            e = int(x[1][11:13])
            li = [t for t in range(24) if t > s if t < e]

            for l in li:
                result.append((date_m6h, int(l), stop_id))

        result = list(set(result))
        result.sort(
            key = lambda x : (x[0], x[2], x[1])
        )

        return result
