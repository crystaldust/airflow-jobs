from oss_know.libs.util.log import logger


# YYYYMM to YYYY, MM
def unwind(year_month_num):
    return int(year_month_num / 100), year_month_num % 100


# num of delta months
def delta_in_month(prev, current):
    year_a, month_a = unwind(prev)
    year_b, month_b = unwind(current)
    return (year_b - year_a) * 12 + (month_b - month_a)


_system = 12


# new date with (date + 1 month)
def add_month(date, months):
    year, month = unwind(date)
    _sum = month + months

    if _sum == _system:
        return year * 100 + _system

    return (year + int(_sum / _system)) * 100 + (_sum % _system)


def make_rows(container, year_month, owner, repo, ids, id_type):
    for _id in ids:
        container.append({
            'year_month': year_month,
            'id_type': id_type,
            'owner': owner,
            'repo': repo,
            'owner_id': _id
        })


# Make up the empty periods of code owners
# Example:
# p0: o1, o2, o3
# p1: no data
# p2: no data
# p3: o3, o4, o5
# Then p1 and p2 should have owners [o1, o2, o3](inherited from p0)
def makeup_periods(owner, repo, id_type, client):
    sql = f'''
    select year_month, groupArray({id_type})
    from (
             select distinct year_month, {id_type}
             from (
                      select toYYYYMM(authored_date) as year_month, {id_type}
                      from code_owners
                      where search_key__owner = '{owner}'
                        and search_key__repo = '{repo}'
                        and {id_type} != ''
                      order by year_month desc
                      )
             )
    group by year_month
    order by year_month
    '''

    results = client.execute_no_params(sql)

    prev_ts = 0
    prev_set = set()
    container = []
    for year_month, ids in results:
        logger.info(f'Loop period {year_month}')
        delta_months = delta_in_month(prev_ts, year_month)
        current_set = set(ids)

        if delta_months != 1 and prev_ts != 0:
            delta_in_month(year_month, prev_ts)
            periods_to_makeup = [add_month(prev_ts, i) for i in range(1, delta_months)]
            logger.info(f'Makeup periods between {prev_ts} to {year_month}, namely, {periods_to_makeup}')

            for period in periods_to_makeup:
                make_rows(container, period, owner, repo, prev_set, id_type)

            make_rows(container, year_month, owner, repo, current_set, id_type)
        else:
            # prev_set = set(prev_ids)
            # only_in_prev = prev_set.difference(current_set)
            # What to do with these? It should be 0, which is weird, the people should not just disappear in a blink
            # union_ids = prev_set.union(set(ids))
            make_rows(container, year_month, owner, repo, current_set, id_type)
            # prev_set = union_ids
        prev_set = current_set
        prev_ts = year_month
        
    return container
