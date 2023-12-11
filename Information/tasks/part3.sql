-- 3.1
create or replace function fnc_transferredpoints()
  returns table (Peer1 varchar, Peer2 varchar, PointsAmount bigint)
as
$$
BEGIN
  RETURN query with tmp as (select t1.id as id_1, t2.id as id_2,
                                   t1.CheckingPeer as Checking_1,
                                   t1.CheckedPeer as Checked_1,
                                   t1.PointsAmount as Points_1,
                                   t2.CheckingPeer as Checking_2,
                                   t2.CheckedPeer as Checked_2,
                                   t2.PointsAmount as Points_2
                            from TransferredPoints t1
                                    left outer join TransferredPoints t2 on t1.CheckedPeer = t2.CheckingPeer
                                      and t1.CheckingPeer = t2.CheckedPeer)
              select tmp.Checking_1, tmp.Checked_1,
                     (coalesce(Points_1, 0) - coalesce(Points_2, 0)) from tmp
                     where (id_1 < id_2)
                        or (id_2 is null);
END;
$$
  language 'plpgsql';

select * from fnc_transferredpoints();

-- 3.2
create or replace function fnc_checks()
    returns table (Peer varchar, Task varchar, XP integer)
as
$$
BEGIN
  return query with tmp_checks as (select Checks.id, Checks.Peer as peer, Checks.Task as checks_task
                                   from Checks
                                          join P2P p on p."Check" = Checks.id and p.State = 'Success'
                                   except all
                                   select Checks.id, Checks.Peer as peer, Checks.Task as checks_task
                                   from Checks
                                          join Verter V on Checks.id = V."Check" and V.State = 'Failure'),
                  tmp_tasks_xp as (select distinct Checks.id, XP.XPAmount as XP, Checks.Task as tasks_title
                                   from Checks join XP on XP."Check" = Checks.id)
                select tmp_checks.peer, checks_task, tmp_tasks_xp.XP
                from tmp_checks join tmp_tasks_xp on tmp_checks.id = tmp_tasks_xp.id
                order by peer;
END;
$$
  language 'plpgsql';

select * from fnc_checks();

-- 3.3
create or replace function fnc_timetracking(in pdate date)
    returns table (Peer varchar)
as
$$
BEGIN
  return query (select TimeTracking.Peer from timetracking
                where State = 1 and "Date" = pdate
                group by TimeTracking.Peer
                having count(State) = 1)
               except all
               ((select TimeTracking.Peer from TimeTracking
                 where State = 1 and "Date" = pdate)
                except all
                (select TimeTracking.Peer from TimeTracking
                 where State = 2 and "Date" = pdate));
END;
$$
  language 'plpgsql';

select * from fnc_timetracking('2021-11-28');

-- 3.4
create or replace function fnc_get_transferredpoints(
  in ch_peer_nickname_in varchar
)
  returns integer
as
$$
declare
  points integer;
BEGIN
  with tmp as (select t1.CheckingPeer, sum(t1.PointsAmount) as num1
               from TransferredPoints t1
               group by t1.CheckingPeer),
      tmp2 as (select t2.CheckedPeer, sum(t2.PointsAmount) as num2
               from TransferredPoints t2
               group by t2.CheckedPeer),
      tmp3 as (select coalesce(tmp.CheckingPeer, tmp2.CheckedPeer) as Peer,
                      (coalesce(num1, 0) - coalesce(num2, 0)) as PointsChange
               from tmp full outer join tmp2 on tmp2.CheckedPeer = tmp.CheckingPeer)
  select PointsChange into points
    from tmp3 where tmp3.Peer = ch_peer_nickname_in;
  return points;
END;
$$
  language plpgsql;

create or replace procedure pr_count_transferredpoints(
  result_data inout refcursor
) language plpgsql as
$$
BEGIN
  open result_data for
    select Nickname as Peer,
           coalesce(fnc_get_transferredpoints(Nickname), 0) as PointsChange
    from Peers
    order by PointsChange desc;
END;
$$;

BEGIN;
CALL pr_count_transferredpoints('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.5
create or replace function fnc_get_points_from_transferredpoints(in ch_peer_nickname_in varchar)
returns integer as
$$
declare
  points integer;
BEGIN
  with tmp as (select t1.Peer1, sum(t1.PointsAmount) as num1
               from fnc_transferredpoints() t1
               group by t1.Peer1),
      tmp2 as (select t2.Peer2, sum(t2.PointsAmount) as num2
               from fnc_transferredpoints() t2
               group by t2.Peer2),
      tmp3 as (select coalesce(tmp.Peer1, tmp2.Peer2) as Peer,
                      (coalesce(num1, 0) - coalesce(num2, 0)) as PointsChange
               from tmp full outer join tmp2 on tmp2.Peer2 = tmp.Peer1)
  select PointsChange into points
    from tmp3 where tmp3.Peer = ch_peer_nickname_in;
  return points;
END;
$$
  language plpgsql;

create or replace procedure pr_count_transferredpoints(
  result_data inout refcursor
) language plpgsql as
$$
BEGIN
  open result_data for
    select Nickname as Peer,
           coalesce(fnc_get_points_from_transferredpoints(Nickname), 0) as PointsChange
    from Peers
    order by PointsChange desc;
END;
$$;

BEGIN;
CALL pr_count_transferredpoints('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.6
create or replace function fnc_get_most_checked(
  in ch_checks_date_in date
)
  returns table (Task varchar)
as
$$
declare
  counter integer;
BEGIN
  select count(Checks.Task) as T
  into counter
  from Checks
  where "Date" = ch_checks_date_in
  group by Checks.Task
  order by T desc
  limit 1;

  return query
    select Checks.Task
    from Checks
    where "Date" = ch_checks_date_in
    group by Checks.Task
    having count(Checks.Task) = counter;
END;
$$
  language plpgsql;

create or replace procedure pr_get_most_checked(
  result_data inout refcursor
) language plpgsql as
$$
BEGIN
  open result_data for
    select distinct "Date" as Day,
      fnc_get_most_checked("Date") as Task
    from Checks
    order by "Date" desc;
END;
$$;

BEGIN;
CALL pr_get_most_checked('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.7
create or replace function fnc_block_done(in block_in varchar)
  returns table (Peer varchar, Day date) as
$$
BEGIN
  return query (with tmp as (select Title from Tasks where Title ~ ('' || block_in || '')),
                    tmp2 as (select Checks.Peer, Checks.Task, Checks."Date", XP.XPAmount
                             from Checks join XP on Checks.ID = XP."Check"
                             where Checks.Task ~ ('' || block_in || '')),
                    tmp3 as (select Nickname, tmp.Title, XPAmount
                             from Peers
                                      cross join tmp
                                      left outer join tmp2 on tmp2.Task = tmp.Title
                                                    and Peers.Nickname = tmp2.Peer),
                    tmp4 as (select Nickname
                             from tmp3
                             except
                             select Nickname
                             from tmp3
                             where XPAmount is null)
              select Nickname, max("Date") as Day
              from tmp4 join tmp2 on tmp2.Peer = Nickname
              group by Nickname
              order by Day desc);
END;
$$
  language plpgsql;

create or replace procedure pr_block_done(
  result_data inout refcursor,
  in block_in varchar) as
$$
BEGIN
  open result_data for
  select * from fnc_block_done(block_in);
END;
$$
  language plpgsql;

BEGIN;
CALL pr_block_done('data', 'DO');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.8
create or replace function fnc_get_recommendation_peer(ch_peers_nickname_in varchar)
  returns varchar as
$$
declare
  answer varchar;
BEGIN
  with tmp as (select Nickname, RecommendedPeer, count(RecommendedPeer) as c
               from Peers
                left outer join Friends on peers.Nickname = Friends.Peer2
                left outer join Recommendations on Friends.Peer1 = Recommendations.Peer
               group by Nickname, RecommendedPeer),
      tmp2 as (select *, rank() over (partition by Nickname order by c desc) as r
               from tmp)
  select RecommendedPeer
  into answer
  from tmp2
  where r = 1
    and Nickname = ch_peers_nickname_in and ch_peers_nickname_in <> RecommendedPeer;
  return answer;
END;
$$
  language plpgsql;

create or replace procedure pr_get_recommendation_peer(result_data inout refcursor)
as
$$
BEGIN
  open result_data for
    select Nickname, fnc_get_recommendation_peer(Nickname) as RecommendedPeer
    from Peers;
END;
$$ language plpgsql;

BEGIN;
CALL pr_get_recommendation_peer('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.9
create or replace function fnc_percent_of_block(in block_f varchar, in block_s varchar)
  returns table (
    StartedBlock1 numeric,
    StartedBlock2 numeric,
    StartedBothBlocks numeric,
    StartedNoBlocks numeric
  )
as
$$
declare
  amount integer;
BEGIN
  select count(Peers.Nickname) into amount from Peers;
  return query (with tmp as (select Peers.Nickname, Checks.Task
                             from Peers
                                left outer join Checks on Peers.Nickname = Checks.Peer),
                    tmp2 as (select distinct tmp.Nickname from tmp where task ~ ('' || block_f || '')),
                    tmp3 as (select distinct tmp.Nickname from tmp where task ~ ('' || block_s || ''))
                select round(((select count(Nickname) from tmp2)::numeric / amount * 100), 2),
                       round(((select count(Nickname) from tmp3)::numeric / amount * 100), 2),
                       round(((select count(tmp2.Nickname)
                               from tmp2
                                        join tmp3 on tmp2.Nickname = tmp3.Nickname)::numeric / amount * 100), 2),
                       round (((select count(Nickname)
                                from (select Nickname
                                      from tmp
                                      except
                                      ((select Nickname from tmp2) union (select Nickname from tmp3))) as tmp5)::numeric /
                                amount * 100), 2));
END;
$$
  language plpgsql;

create or replace procedure pr_percent_of_block(result_data inout refcursor, in block_f varchar, in block_s varchar) as
$$
BEGIN
  open result_data for
    select *
    from fnc_percent_of_block(block_f, block_s);
END;
$$
  language plpgsql;

BEGIN;
CALL pr_percent_of_block('data', 'C', 'DO');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.10
create or replace procedure pr_percent_of_checks_on_birthday(
  inout SuccessfulChecks numeric,
  inout UnsuccessfulChecks numeric
) as
$$
declare
  amount integer;
BEGIN
  select count(ID)
  into amount
  from Checks
    join Peers on Peers.Nickname = Checks.Peer
  where to_char(Checks."Date"::date, 'mm-dd') =
        to_char(Birthday::date, 'mm-dd');
  select (select (round(100 * count(*)::numeric / amount , 2))
          from (select distinct Checks.ID
                from Checks
                    join P2P p on p."Check" = Checks.ID and p.State = 'Success'
                    join Verter on Checks.ID = Verter."Check" and Verter.State = 'Success'
                    join Peers on Peers.Nickname = Checks.Peer
                where to_char(Checks."Date"::date, 'mm-dd') =
                      to_char(Birthday::date, 'mm-dd')) as tmp) as SuccessfulChecks,
         (select (round(100 * count(*)::numeric / amount, 2))
          from (select distinct Checks.ID, Checks.Peer, Checks.Task, 'Failure' as status
                from Checks
                  left outer join P2P p on p."Check" = Checks.ID
                  left outer join Verter on Checks.ID = Verter."Check"
                  left outer join Peers on Peers.Nickname = Checks.Peer
                where (p.State = 'Failure'
                  or Verter.State = 'Failure')
                  and (to_char(Checks."Date"::date, 'mm-dd') =
                       to_char(Birthday::date, 'mm-dd'))) as tmp) as UnsuccessfulChecks
  into SuccessfulChecks, UnsuccessfulChecks;
END;
$$
  language plpgsql;

CALL pr_percent_of_checks_on_birthday(0, 0);

-- 3.11
create or replace procedure pr_pass_all_but_third_project(FirstProject varchar, SecondProject varchar, ThirdProject varchar,
                                                          result_data inout refcursor)
as
$$
BEGIN
  open result_data for (select Peer
                        from Checks
                        where Task = FirstProject
                        intersect
                        select Peer
                        from Checks
                        where Task = SecondProject
                        except
                        select Peer
                        from Checks
                        where Task = ThirdProject);
END;
$$
  language plpgsql;

BEGIN;
CALL pr_pass_all_but_third_project('C3_SimpleBashUtils', 'DO1_Linux', 'C4_math', 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.12
create or replace procedure pr_path_to_project(result_data inout refcursor)
as
$$
BEGIN
  open result_data for
    (with recursive task_access_path(Task, PrevCount) as (select Title, 0
                                                          from Tasks
                                                          where ParentTask is null
                                                          union all
                                                          select Title, PrevCount + 1
                                                          from task_access_path, Tasks
                                                          where task_access_path.Task = Tasks.ParentTask)
     select Task, PrevCount
     from task_access_path);
END;
$$
  language plpgsql;

BEGIN;
CALL pr_path_to_project('data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.13
create or replace function fnc_lucky_days(n_day integer)
  returns table (
    Date date
  )
as
$$
declare
  count_success int := 0;
  prev_d date := (select min("Date") from Checks);
  value record;
  today bool := false;
  l_cur cursor for (select p.State as cur_state,
                           c."Date" as cur_date,
                           p2."Time" as cur_time
                    from P2P p
                            join Checks c on c."ID" = p."Check"
                            join P2P p2 on c."ID" = p2."Check" and p2.State = 'Start'
                    where p.State != 'Start'
                    order by cur_date, cur_time);
BEGIN
  for value in l_cur
    loop
      if value.cur_date != prev_d then
        count_success = count_success + 1;
        if count_success = n_day then
          count_success = 0;
          Date = value.cur_date;
          today = true;
          return next;
        end if;
      else
        count_success = 0;
      end if;
      prev_d = value.cur_date;
    end loop;
END;
$$
  language plpgsql;

create or replace procedure pr_lucky_days(count_checks integer, result_data inout refcursor)
as
$$
BEGIN
  open result_data for
    select * from fnc_lucky_days(count_checks);
END;
$$
  language plpgsql;

BEGIN;
CALL pr_lucky_days(1, 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.14
create or replace procedure pr_max_xp_peer() as
$$
BEGIN
  create or replace view local_table_max_xp_peer(Peer, Task, XPAmount, r) as
  (
    select Peer, Task, XPAmount, rank() over (partition by Peer, Task order by XPAmount desc)
    from Checks join XP x on Checks.id = x."Check"
  );
END;
$$
  language plpgsql;

CALL pr_max_xp_peer();

select Peer, sum(XPAmount) as XP
from local_table_max_xp_peer
where r = 1
group by Peer
order by XP desc
limit 1;

-- 3.15
create or replace procedure pr_peers_came_early(cur_time time, cur_count integer, result_data inout refcursor) as
$$
BEGIN
  open result_data for
    select Peer
    from TimeTracking
    where "Time" < cur_time and State = 1
    group by Peer
    having count(State) >= cur_count;
END;
$$
  language plpgsql;

BEGIN;
CALL pr_peers_came_early('21:00:00', '2', 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.16
create or replace procedure pr_peers_left_campus(days_number integer, out_number integer, result_data inout refcursor) as
$$
BEGIN
  open result_data for
    select Peer
    from TimeTracking
    where "Date" between current_date - days_number and current_date
      and State = 2
    group by Peer
    having count(State) > out_number;
END;
$$
  language plpgsql;

BEGIN;
CALL pr_peers_left_campus(550, 1, 'data');
FETCH ALL IN "data";
COMMIT;
END;

-- 3.17
create or replace function fnc_get_early_entries_percent()
  returns table (
    Month text,
    EarlyEntries numeric
  )
as
$$
BEGIN
  return query (with came_on_birthday as (select distinct Nickname, date_part('month', Birthday) as birthday_month
                                          from Peers),
                      get_login_month as (select distinct date_part('month', "Date") as login_month, Peer as login_peer
                                          from TimeTracking),
       get_number_login_peer_in_month as (select count(State) as count_login, Peer as t_peer
                                          from TimeTracking
                                          where State = 1
                                          group by t_peer),
                 login_in_birth_month as (select Nickname, count_login
                                          from came_on_birthday as c
                                                              join get_login_month on Nickname = login_peer and birthday_month = login_month
                                                              join get_number_login_peer_in_month g on g.t_peer = c.Nickname),
                 total_login_in_month as (select sum(count_login) as total_login from login_in_birth_month), -- part 1
                      get_early_login as (select distinct sum(State) as total_early_login, "Date" as early_date
                                          from login_in_birth_month as l
                                                                    join TimeTracking t on t.Peer = l.Nickname and t."Time" < '12:00'
                                          group by early_date), -- part 2
                  percent_early_login as (select distinct to_char("Date", 'Month') as tmp_m,
                                           (100 / total_login) * total_early_login as tmp_p
                                          from TimeTracking
                                            cross join total_login_in_month
                                            join get_early_login gel on date_part('month', TimeTracking."Date") = date_part('month', gel.early_date)) -- part 3
      select tmp_m, tmp_p
      from percent_early_login);
END;
$$
  language plpgsql;

create or replace procedure pr_login_in_every_month(result_data inout refcursor) as
$$
BEGIN
  open result_data for
    select *
    from fnc_get_early_entries_percent();
END;
$$
  language plpgsql;

BEGIN;
CALL pr_login_in_every_month('data');
FETCH ALL IN "data";
COMMIT;
END;
